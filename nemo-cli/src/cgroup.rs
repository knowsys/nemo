//! Control resource usage using CGroups

use zbus::{
    Connection, Result, proxy,
    zvariant::{Array, OwnedObjectPath, Signature, Str, Value},
};

#[proxy(
    interface = "org.freedesktop.systemd1.Manager",
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
trait SystemdManager {
    async fn start_transient_unit(
        &self,
        name: &str,
        mode: &str,
        properties: &[&(&str, &Value<'_>)],
        aux: &[&(&str, &[&(&str, &Value<'_>)])],
    ) -> Result<OwnedObjectPath>;
}

pub(super) struct CgroupBuilder<'a> {
    unit: String,
    pids: Array<'a>,
    slice: Option<&'a str>,
    time_limit: Option<u64>,
    memory_limit: Option<u64>,
}

impl CgroupBuilder<'_> {
    pub(super) fn new(unit_template: &str) -> Self {
        let pid = std::process::id();
        let unit = unit_template.replace("{pid}", &format!("{pid}"));
        let mut pids = Array::new(&Signature::U32);
        pids.append(Value::U32(pid))
            .expect("value matches signature");

        Self {
            unit,
            pids,
            slice: None,
            time_limit: None,
            memory_limit: None,
        }
    }

    pub(super) fn time_limit(&mut self, seconds: u32) -> &mut Self {
        self.time_limit = Some(1_000_000 * u64::from(seconds));
        self
    }

    pub(super) fn memory_limit(&mut self, mebibytes: u32) -> &mut Self {
        self.memory_limit = Some(1_048_576 * u64::from(mebibytes));
        self
    }

    pub(super) async fn create_scope(self) -> Result<()> {
        let mut props = vec![
            (
                "Slice",
                Value::Str(Str::from(self.slice.unwrap_or("app.slice"))),
            ),
            ("PIDs", Value::Array(self.pids)),
            (
                "Description",
                Value::Str(Str::from(format!(
                    "Nemo graph rule engine -- process {}",
                    std::process::id(),
                ))),
            ),
            ("Delegate", Value::Bool(true)),
            ("DefaultDependencies", Value::Bool(true)),
            ("IPAccounting", Value::Bool(true)),
            ("MemoryAccounting", Value::Bool(true)),
            ("TasksAccounting", Value::Bool(true)),
            ("IOAccounting", Value::Bool(true)),
            ("OOMPolicy", Value::Str(Str::from("kill"))),
        ];

        if let Some(microseconds) = self.time_limit {
            props.push(("RuntimeMaxUSec", Value::U64(microseconds)));
        }

        if let Some(bytes) = self.memory_limit {
            props.push(("MemoryMax", Value::U64(bytes)));
            props.push(("MemorySwapMax", Value::U64(0)));
            props.push(("MemoryZSwapMax", Value::U64(0)));
        }

        // borrow, since we need to have references to values in order to call the bus method
        let props_borrowed = props.iter().map(|(k, v)| (*k, v)).collect::<Vec<_>>();
        // re-borrow, since we also need to have references to tuples
        let props_borrowed = props_borrowed.iter().collect::<Vec<_>>();

        let connection = Connection::session().await?;
        let proxy = SystemdManagerProxy::new(&connection).await?;
        let reply = proxy
            .start_transient_unit(&self.unit, "replace", &props_borrowed, &[])
            .await?;
        log::debug!("Created cgroup {reply}");

        Ok(())
    }
}
