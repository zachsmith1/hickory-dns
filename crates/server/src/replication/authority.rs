//! Hickory authority integration for replicated zones.
//!
//! This implements a minimal [`ZoneHandler`] backed by the active `redb` slot.
//! The handler auto-refreshes when the `ACTIVE` pointer flips.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::proto::serialize::binary::{BinDecoder, Restrict};
use crate::proto::{
    op::ResponseCode,
    rr::{LowerName, Name, RData, RecordSet, RecordType},
};
use crate::server::RequestInfo;
use crate::zone_handler::{
    AuthLookup, AxfrPolicy, LookupControlFlow, LookupError, LookupOptions, LookupRecords,
    ZoneHandler, ZoneType,
};

use super::redb_store::{RedbActiveView, Slot, ZoneLayout};

struct ActiveState {
    slot: Slot,
    view: RedbActiveView,
}

/// A zone handler that serves from a replicated `redb` active view.
pub struct RedbReplicatedZoneHandler {
    origin: LowerName,
    zone_type: ZoneType,
    axfr_policy: AxfrPolicy,
    layout: ZoneLayout,
    active: RwLock<Option<ActiveState>>,
}

impl RedbReplicatedZoneHandler {
    /// Create a new handler for a zone origin and on-disk layout.
    pub fn new(origin: Name, zone_type: ZoneType, axfr_policy: AxfrPolicy, layout: ZoneLayout) -> Self {
        Self {
            origin: LowerName::new(&origin),
            zone_type,
            axfr_policy,
            layout,
            active: RwLock::new(None),
        }
    }

    async fn with_view<R>(
        &self,
        f: impl FnOnce(&RedbActiveView) -> Result<R, LookupError>,
    ) -> Result<R, LookupError> {
        let current_slot = self.layout.active_slot();

        // Fast path: already loaded and matches.
        {
            let guard = self.active.read().await;
            if let Some(state) = guard.as_ref() {
                if state.slot == current_slot {
                    return f(&state.view);
                }
            }
        }

        // Slow path: write lock and refresh.
        let mut guard = self.active.write().await;
        if let Some(state) = guard.as_ref() {
            if state.slot == current_slot {
                return f(&state.view);
            }
        }

        let view = RedbActiveView::open(self.layout.clone())
            .map_err(|_| LookupError::from(ResponseCode::ServFail))?;
        *guard = Some(ActiveState {
            slot: current_slot,
            view,
        });

        // Now safe to read it back under the same guard.
        let state = guard.as_ref().expect("just set");
        f(&state.view)
    }

    fn rrset_from_wire(
        owner: &LowerName,
        rrtype: RecordType,
        ttl: u32,
        rdata_wire: &[Vec<u8>],
    ) -> Result<Arc<RecordSet>, LookupError> {
        let mut rrset = RecordSet::new(owner.clone().into(), rrtype, ttl);
        for bytes in rdata_wire {
            let mut decoder = BinDecoder::new(bytes);
            let rdata = RData::read(&mut decoder, rrtype, Restrict::new(bytes.len() as u16))
                .map_err(|_| LookupError::from(ResponseCode::ServFail))?;
            rrset.add_rdata(rdata);
        }
        Ok(Arc::new(rrset))
    }
}

#[async_trait::async_trait]
impl ZoneHandler for RedbReplicatedZoneHandler {
    fn zone_type(&self) -> ZoneType {
        self.zone_type
    }

    fn axfr_policy(&self) -> AxfrPolicy {
        self.axfr_policy
    }

    fn origin(&self) -> &LowerName {
        &self.origin
    }

    async fn lookup(
        &self,
        name: &LowerName,
        rtype: RecordType,
        _request_info: Option<&RequestInfo<'_>>,
        lookup_options: LookupOptions,
    ) -> LookupControlFlow<AuthLookup> {
        if rtype == RecordType::AXFR {
            return LookupControlFlow::Break(Err(LookupError::NetError(
                "AXFR must be handled with ZoneHandler::zone_transfer()".into(),
            )));
        }

        // Only answer for names under the zone.
        if !self.origin.zone_of(&name.clone().into()) {
            return LookupControlFlow::Continue(Err(LookupError::from(ResponseCode::Refused)));
        }

        let owner_fqdn = name.to_string();
        let rrtype_u16: u16 = rtype.into();

        let rrset = match self
            .with_view(|view| {
                let rrset = view
                    .get_rrset(&owner_fqdn, rrtype_u16)
                    .map_err(|_| LookupError::from(ResponseCode::ServFail))?;

                let Some(rrset) = rrset else {
                    // Distinguish NODATA (name exists) vs NXDOMAIN (name does not exist).
                    return if view
                        .owner_exists(&owner_fqdn)
                        .map_err(|_| LookupError::from(ResponseCode::ServFail))?
                    {
                        Err(LookupError::for_name_exists())
                    } else {
                        Err(LookupError::from(ResponseCode::NXDomain))
                    };
                };

                let decoded = Self::rrset_from_wire(name, rtype, rrset.ttl, &rrset.rdata_wire)?;
                Ok(Some(decoded))
            })
            .await
        {
            Ok(Some(r)) => r,
            Ok(None) => unreachable!("missing RRset should return a LookupError"),
            Err(e) => return LookupControlFlow::Continue(Err(e)),
        };

        LookupControlFlow::Continue(Ok(AuthLookup::answers(
            LookupRecords::new(lookup_options, rrset),
            None,
        )))
    }

    async fn search(
        &self,
        request: &crate::server::Request,
        lookup_options: LookupOptions,
    ) -> (
        LookupControlFlow<AuthLookup>,
        Option<Box<dyn crate::proto::op::ResponseSigner>>,
    ) {
        let request_info = match request.request_info() {
            Ok(info) => info,
            Err(e) => return (LookupControlFlow::Break(Err(e)), None),
        };

        let lookup_name = request_info.query.name();
        let record_type: RecordType = request_info.query.query_type();
        (
            self.lookup(lookup_name, record_type, Some(&request_info), lookup_options)
                .await,
            None,
        )
    }

    async fn nsec_records(
        &self,
        _name: &LowerName,
        _lookup_options: LookupOptions,
    ) -> LookupControlFlow<AuthLookup> {
        LookupControlFlow::Continue(Ok(AuthLookup::default()))
    }

    #[cfg(feature = "__dnssec")]
    async fn nsec3_records(
        &self,
        _info: crate::zone_handler::Nsec3QueryInfo<'_>,
        _lookup_options: LookupOptions,
    ) -> LookupControlFlow<AuthLookup> {
        LookupControlFlow::Continue(Ok(AuthLookup::default()))
    }

    #[cfg(feature = "metrics")]
    fn metrics_label(&self) -> &'static str {
        "replication-redb"
    }
}

#[cfg(test)]
mod tests {
    use super::RedbReplicatedZoneHandler;
    use crate::replication::redb_store::{encode_rrset_value, rrset_key, RedbStaging, ZoneLayout};
    use crate::replication::snapshot::{ZoneSnapshot, CURRENT_SNAPSHOT_FORMAT_VERSION};
    use crate::zone_handler::{
        AxfrPolicy, LookupControlFlow, LookupError, LookupOptions, ZoneHandler, ZoneType,
    };
    use crate::proto::rr::{LowerName, Name, RecordType};
    use std::collections::BTreeMap;

    struct TempDir {
        path: std::path::PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let mut path = std::env::temp_dir();
            let nonce = format!(
                "{prefix}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            path.push(nonce);
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[tokio::test]
    async fn serves_a_record_from_active_redb() {
        let dir = TempDir::new("hickory-redb-zonehandler");
        let layout = ZoneLayout::new(dir.path.join("zone"));

        // Stage snapshot into inactive slot and activate it.
        let mut entries = BTreeMap::new();
        let k = rrset_key("www.example.com.", u16::from(RecordType::A));
        // A RDATA is exactly 4 bytes.
        let v = encode_rrset_value(&crate::replication::delta::RrsetData {
            ttl: 60,
            rdata_wire: vec![vec![1, 2, 3, 4]],
        });
        entries.insert(k, v);
        let snap = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 1,
            entries,
        };

        let mut staging = RedbStaging::open_inactive(layout.clone()).unwrap();
        staging.load_snapshot(&snap, 1).unwrap();
        staging.activate().unwrap();

        let handler = RedbReplicatedZoneHandler::new(
            Name::from_ascii("example.com.").unwrap(),
            ZoneType::Primary,
            AxfrPolicy::Deny,
            layout,
        );

        let qname = LowerName::from(&Name::from_ascii("www.example.com.").unwrap());
        let res = handler
            .lookup(&qname, RecordType::A, None, LookupOptions::default())
            .await;

        match res {
            LookupControlFlow::Continue(Ok(lookup)) => {
                assert!(!lookup.was_empty());
            }
            other => panic!("unexpected lookup result: {other}"),
        }
    }

    #[tokio::test]
    async fn missing_type_returns_name_exists_when_owner_has_other_rrsets() {
        let dir = TempDir::new("hickory-redb-zonehandler-nodata");
        let layout = ZoneLayout::new(dir.path.join("zone"));

        // Seed with only A at www.
        let mut entries = BTreeMap::new();
        let k = rrset_key("www.example.com.", u16::from(RecordType::A));
        let v = encode_rrset_value(&crate::replication::delta::RrsetData {
            ttl: 60,
            rdata_wire: vec![vec![1, 2, 3, 4]],
        });
        entries.insert(k, v);
        let snap = ZoneSnapshot {
            format_version: CURRENT_SNAPSHOT_FORMAT_VERSION,
            zone_id: "z1".into(),
            origin: "example.com.".into(),
            base_generation: 1,
            entries,
        };

        let mut staging = RedbStaging::open_inactive(layout.clone()).unwrap();
        staging.load_snapshot(&snap, 1).unwrap();
        staging.activate().unwrap();

        let handler = RedbReplicatedZoneHandler::new(
            Name::from_ascii("example.com.").unwrap(),
            ZoneType::Primary,
            AxfrPolicy::Deny,
            layout,
        );

        let qname = LowerName::from(&Name::from_ascii("www.example.com.").unwrap());
        let res = handler
            .lookup(&qname, RecordType::AAAA, None, LookupOptions::default())
            .await;

        match res {
            LookupControlFlow::Continue(Err(LookupError::NameExists)) => {}
            other => panic!("unexpected lookup result: {other}"),
        }
    }
}

