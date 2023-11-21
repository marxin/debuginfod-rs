use std::collections::HashMap;

use anyhow::Result;
use rayon::prelude::*;
use rpm;
use std::sync::mpsc::channel;
use walkdir::WalkDir;

const ARCH_MAPPING: [&str; 1] = ["x86_64"];
const DEBUG_INFO_PATH_PREFIX: &str = "/usr/lib/debug/.build-id/";

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct RPMSourceKey {
    arch: usize,
    source_rpm: String,
}

#[derive(Debug)]
enum RPMContent {
    Binary,
    DebugInfo { build_ids: HashMap<String, String> },
    DebugSource,
}

#[derive(Debug)]
struct RPMFile {
    source: RPMSourceKey,
    path: String,
    content: RPMContent,
}

struct Server {
    root_path: String,
    binary_rpms: HashMap<RPMSourceKey, RPMFile>,
    debug_info_rpms: HashMap<RPMSourceKey, RPMFile>,
    debug_source_rpms: HashMap<RPMSourceKey, RPMFile>,
}

impl Server {
    fn new(root_folder: &str) -> Self {
        Server {
            root_path: String::from(root_folder),
            binary_rpms: HashMap::new(),
            debug_info_rpms: HashMap::new(),
            debug_source_rpms: HashMap::new(),
        }
    }

    fn walk(&mut self) {
        let mut files = Vec::new();
        for entry in WalkDir::new(self.root_path.clone()) {
            let entry = entry.unwrap();
            if entry.metadata().unwrap().is_file() {
                files.push(String::from(entry.path().to_str().unwrap()));
            }
        }

        let (rx, tx) = channel();

        files.par_iter().for_each_with(rx, |rx, path| {
            let _ = rx.send(self.analyze_file(path));
        });

        for item in tx.iter() {
            if let Ok(rpm_file) = item {
                let map = match rpm_file.content {
                    RPMContent::Binary => &mut self.binary_rpms,
                    RPMContent::DebugInfo { .. } => &mut self.debug_info_rpms,
                    RPMContent::DebugSource => &mut self.debug_source_rpms,
                };
                map.insert(rpm_file.source.clone(), rpm_file);
            } else {
                todo!();
            }
        }
    }

    fn analyze_file(&self, path: &str) -> Result<RPMFile> {
        let rpm_file = std::fs::File::open(path)?;
        let mut buf_reader = std::io::BufReader::new(rpm_file);
        // TODO: use ?
        let header = rpm::PackageMetadata::parse(&mut buf_reader).unwrap();

        let name = header.get_name().unwrap();
        let source_rpm = String::from(header.get_source_rpm().unwrap());
        let arch = header.get_arch().unwrap();
        let source = RPMSourceKey {
            arch: ARCH_MAPPING.iter().position(|&item| item == arch).unwrap(),
            source_rpm,
        };
        let path = String::from(path);

        let is_debug_info_rpm = name.ends_with("-debuginfo");
        let mut build_ids = HashMap::new();

        // TODO: called `Result::unwrap()` on an `Err` value: TagNotFound("RPMTAG_FILEMODES")

        if let Ok(entries) = header.get_file_entries() {
            for file_entry in entries {
                let path = String::from(file_entry.path.to_str().unwrap());
                if is_debug_info_rpm {
                    if path.starts_with(DEBUG_INFO_PATH_PREFIX) && path.ends_with(".debug") {
                        let components: Vec<_> = file_entry
                            .path
                            .components()
                            .rev()
                            .take(2)
                            .map(|path| path.as_os_str().to_str().unwrap())
                            .collect();
                        let first = components[1];
                        let second = components[0].replace(".debug", "");
                        let mut build_id = String::from(first);
                        build_id.push_str(second.as_str());
                        build_ids.insert(build_id, path.clone());
                    }
                }
            }
        }

        let content = if is_debug_info_rpm {
            RPMContent::DebugInfo { build_ids }
        } else if name.ends_with("-debugsource") {
            RPMContent::DebugSource
        } else {
            RPMContent::Binary
        };
        Ok(RPMFile {
            source,
            path,
            content,
        })
    }
}

fn main() {
    let mut server = Server::new("/home/marxin/Data");
    server.walk();
    println!("binaries: {}", server.binary_rpms.len());
    println!("debuginfos: {}", server.debug_info_rpms.len());
    println!("sources: {}", server.debug_source_rpms.len());

    const N: usize = 4;
    for (_, rpm) in server.debug_info_rpms.iter().take(N) {
        println!("{:?}", rpm);
    }
    println!();
    for (_, rpm) in server.debug_source_rpms.iter().take(N) {
        println!("{:?}", rpm);
    }
    println!();
    for (_, rpm) in server.binary_rpms.iter().take(N) {
        println!("{:?}", rpm);
    }
}
