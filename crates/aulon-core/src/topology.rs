//! L3-cache-aware topology discovery.
//!
//! Each [`Shard`] is one L3 cache domain; the broker spawns one worker
//! per shard and pins its OS thread to one of the shard's logical CPUs.
//! Discovery uses Linux `sysfs` directly (no system C dependency); on
//! non-Linux platforms or when sysfs is unavailable, [`detect`] returns
//! a single-shard topology whose `cpus` is the full set returned by
//! `std::thread::available_parallelism`.
//!
//! See `docs/design/topology-sharding.md`.

use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

/// One L3 cache domain.
#[derive(Debug, Clone)]
pub struct Shard {
    /// Stable identifier assigned at discovery time, dense in `0..N`.
    pub shard_id: u32,
    /// Logical CPU ids that share this L3 cache.
    pub cpus: Vec<u32>,
}

/// Topology discovered at startup.
#[derive(Debug, Clone)]
pub struct Topology {
    /// One entry per L3 cache domain, in ascending `shard_id` order.
    pub shards: Vec<Shard>,
}

impl Topology {
    /// Single shard covering all available CPUs. Used as a fallback
    /// and on non-Linux dev hosts.
    #[must_use]
    pub fn single() -> Self {
        let n = std::thread::available_parallelism()
            .unwrap_or_else(|_| NonZeroUsize::new(1).expect("1 is non-zero"))
            .get();
        let cpus: Vec<u32> = (0..n)
            .map(|i| u32::try_from(i).unwrap_or(u32::MAX))
            .collect();
        Self {
            shards: vec![Shard { shard_id: 0, cpus }],
        }
    }

    /// Number of shards.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

/// Discover the L3 topology of the current machine.
///
/// Returns [`Topology::single`] on any platform where sysfs is missing
/// or returns no L3 information.
#[must_use]
pub fn detect() -> Topology {
    detect_in(Path::new("/sys/devices/system/cpu")).unwrap_or_else(Topology::single)
}

/// Same as [`detect`] but parameterised on the sysfs root for testing.
fn detect_in(cpu_root: &Path) -> Option<Topology> {
    if !cpu_root.is_dir() {
        return None;
    }

    let mut by_canonical: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    let mut any_cpu_seen = false;

    for entry in fs::read_dir(cpu_root).ok()? {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let name = name.to_str()?;
        let Some(cpu_id) = parse_cpu_dir_name(name) else {
            continue;
        };
        any_cpu_seen = true;

        let cache_root = entry.path().join("cache");
        let Some(shared) = read_l3_shared_list(&cache_root) else {
            continue;
        };
        let Some(canonical) = shared.iter().min().copied() else {
            continue;
        };
        let bucket = by_canonical.entry(canonical).or_default();
        if !bucket.contains(&cpu_id) {
            bucket.push(cpu_id);
        }
    }

    if by_canonical.is_empty() {
        if any_cpu_seen {
            return None;
        }
        return None;
    }

    let mut shards = Vec::with_capacity(by_canonical.len());
    for (idx, (_canonical, mut cpus)) in by_canonical.into_iter().enumerate() {
        cpus.sort_unstable();
        shards.push(Shard {
            shard_id: u32::try_from(idx).unwrap_or(u32::MAX),
            cpus,
        });
    }
    Some(Topology { shards })
}

fn parse_cpu_dir_name(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("cpu")?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    rest.parse().ok()
}

fn read_l3_shared_list(cache_root: &Path) -> Option<Vec<u32>> {
    let entries = fs::read_dir(cache_root).ok()?;
    for entry in entries {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let name = name.to_str()?;
        if !name.starts_with("index") {
            continue;
        }
        let level_path = entry.path().join("level");
        let Ok(level_str) = fs::read_to_string(&level_path) else {
            continue;
        };
        if level_str.trim() != "3" {
            continue;
        }
        let shared_path = entry.path().join("shared_cpu_list");
        let shared_str = fs::read_to_string(&shared_path).ok()?;
        return Some(parse_cpu_list(shared_str.trim()));
    }
    None
}

/// Parses a Linux cpu-list string like `0-3,8,12-15`.
fn parse_cpu_list(s: &str) -> Vec<u32> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = part.split_once('-') {
            let (Ok(lo), Ok(hi)) = (lo.parse::<u32>(), hi.parse::<u32>()) else {
                continue;
            };
            for v in lo..=hi {
                out.push(v);
            }
        } else if let Ok(v) = part.parse::<u32>() {
            out.push(v);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn parse_cpu_list_simple() {
        assert_eq!(parse_cpu_list("0"), vec![0]);
        assert_eq!(parse_cpu_list("0-3"), vec![0, 1, 2, 3]);
        assert_eq!(parse_cpu_list("0-1,4,6-7"), vec![0, 1, 4, 6, 7]);
        assert_eq!(parse_cpu_list(""), Vec::<u32>::new());
    }

    #[test]
    fn parse_cpu_dir_recognises_only_cpu_n() {
        assert_eq!(parse_cpu_dir_name("cpu0"), Some(0));
        assert_eq!(parse_cpu_dir_name("cpu42"), Some(42));
        assert_eq!(parse_cpu_dir_name("cpufreq"), None);
        assert_eq!(parse_cpu_dir_name("cpuidle"), None);
        assert_eq!(parse_cpu_dir_name("cpu"), None);
        assert_eq!(parse_cpu_dir_name("not_a_cpu"), None);
    }

    #[test]
    fn detect_falls_back_to_single_when_root_missing() {
        let nope = Path::new("/definitely/does/not/exist/aulon-test");
        assert!(detect_in(nope).is_none());
    }

    #[test]
    fn single_topology_has_one_shard_with_some_cpus() {
        let t = Topology::single();
        assert_eq!(t.shards.len(), 1);
        assert!(!t.shards[0].cpus.is_empty());
    }

    #[test]
    fn detect_in_groups_two_l3_domains() {
        // Build a sysfs-shaped fixture: 4 CPUs, 2 L3 domains (0-1, 2-3).
        let tmp = tempdir();
        let root = tmp.path();
        for cpu in 0..4u32 {
            let l3_share = if cpu < 2 { "0-1" } else { "2-3" };
            let cache = root.join(format!("cpu{cpu}/cache"));
            // Index0/Index1 are L1/L2 noise; Index3 is L3.
            for (idx, level) in [(0u32, "1"), (1, "2"), (3, "3")] {
                let dir = cache.join(format!("index{idx}"));
                fs::create_dir_all(&dir).expect("mkdir fixture");
                fs::write(dir.join("level"), level).expect("write level");
                if level == "3" {
                    fs::write(dir.join("shared_cpu_list"), l3_share).expect("write shared");
                }
            }
        }
        // Add some non-cpu noise to confirm the filter holds.
        fs::create_dir_all(root.join("cpufreq")).expect("mkdir cpufreq");
        fs::create_dir_all(root.join("cpuidle")).expect("mkdir cpuidle");

        let t = detect_in(root).expect("fixture is well-formed");
        assert_eq!(t.shard_count(), 2);
        assert_eq!(t.shards[0].cpus, vec![0, 1]);
        assert_eq!(t.shards[1].cpus, vec![2, 3]);
        assert_eq!(t.shards[0].shard_id, 0);
        assert_eq!(t.shards[1].shard_id, 1);
    }

    /// Minimal scoped tempdir helper (avoids pulling in the `tempfile`
    /// dev-dep for one test).
    struct ScopedDir(std::path::PathBuf);
    impl ScopedDir {
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for ScopedDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir() -> ScopedDir {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("aulon-topology-test-{nanos}"));
        fs::create_dir_all(&dir).expect("mkdir scratch");
        ScopedDir(dir)
    }
}
