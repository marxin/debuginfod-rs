use std::collections::HashMap;
use std::sync::mpsc::channel;

use anyhow::Result;
use path_absolutize::*;
use rayon::prelude::*;
use rocket::response::status::{self, NotFound};
use rpm;
use walkdir::WalkDir;

#[macro_use] extern crate rocket;
use rocket::State;

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
    build_ids: HashMap<String, RPMSourceKey>,
}

impl Server {
    fn new(root_folder: &str) -> Self {
        Server {
            root_path: String::from(root_folder),
            binary_rpms: HashMap::new(),
            debug_info_rpms: HashMap::new(),
            debug_source_rpms: HashMap::new(),
            build_ids: HashMap::new(),
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

        // save all build-ids for the future look up
        for (source_key, rpm) in &self.debug_info_rpms {
            if let RPMContent::DebugInfo { build_ids } = &rpm.content {
                for (build_id, _) in build_ids {
                    // TODO: remove clonning
                    self.build_ids.insert(build_id.clone(), source_key.clone());
                }
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
                        let path = file_entry.path;
                        let mut build_id = String::from(
                            path.parent()
                                .unwrap()
                                .file_name()
                                .unwrap()
                                .to_str()
                                .unwrap(),
                        );
                        build_id.push_str(path.file_stem().unwrap().to_str().unwrap());

                        let target = path.parent().unwrap().join(file_entry.linkto.clone());
                        build_ids.insert(
                            build_id,
                            String::from(target.as_path().absolutize().unwrap().to_str().unwrap()),
                        );
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

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[get("/buildid/<build_id>/debuginfo")]
fn debuginfo(build_id: String, state: &State<Server>) -> Result<String, NotFound<&str>> {
    if let Some(path) = state.build_ids.get(&build_id) {
        Ok(state.debug_info_rpms[path].path.clone())
    } else {
        Err(NotFound("The provided build-id is not found."))
    }
}


#[launch]
fn rocket() -> _ {
    let mut server = Server::new("/home/marxin/Data");
    server.walk();
    rocket::build()
        .manage(server)
        .mount("/", routes![index, debuginfo])
}
