use std::{collections::HashMap, hash::Hash};

use rpm;

use walkdir::WalkDir;

use anyhow::Result;

#[derive(Debug)]
enum RPMContent {
    Binary,
    DebugInfo,
    DebugSource,
}

#[derive(Debug)]
struct RPMFile {
    path: String,
    content: RPMContent,
}

struct Server {
    root_path: String,
    binary_rpms: HashMap<String, RPMFile>,
    debug_info_rpms: HashMap<String, RPMFile>,
    debug_source_rpms: HashMap<String, RPMFile>,
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
        for entry in WalkDir::new(self.root_path.clone()) {
            let entry = entry.unwrap();
            if entry.metadata().unwrap().is_file() {
                let path = entry.path().to_str().unwrap();
                if let Ok((source, rpm_file)) = self.analyze_file(path) {
                    let map = match rpm_file.content {
                        RPMContent::Binary => &mut self.binary_rpms,
                        RPMContent::DebugInfo => &mut self.debug_info_rpms,
                        RPMContent::DebugSource => &mut self.debug_source_rpms,
                    };
                    map.insert(source, rpm_file);
                } else {
                    todo!();
                }
            }
        }
    }

    fn analyze_file(&self, path: &str) -> Result<(String, RPMFile)> {
        let rpm_file = std::fs::File::open(path)?;
        let mut buf_reader = std::io::BufReader::new(rpm_file);
        // TODO: use ?
        let header = rpm::RPMPackageMetadata::parse(&mut buf_reader)
            .unwrap()
            .header;

        let name = header.get_name().unwrap();
        let source = String::from(header.get_source_rpm().unwrap());
        let path = String::from(path);

        let content = if name.ends_with("-debuginfo") {
            RPMContent::DebugInfo
        } else if name.ends_with("-debugsource") {
            RPMContent::DebugSource
        } else {
            RPMContent::Binary
        };
        Ok((source, RPMFile { path, content }))
    }
}

fn main() {
    let mut server = Server::new("/home/marxin/Data");
    server.walk();
    println!("binaries: {}", server.binary_rpms.len());
    println!("debuginfos: {}", server.debug_info_rpms.len());
    println!("sources: {}", server.debug_source_rpms.len());

    /*
    for entry in folder.unwrap() {
        entries.push(entry.unwrap());
    }

    for entry in folder2.unwrap() {
        entries.push(entry.unwrap());
    }

    println!("Folders read: {}", entries.len());
    entries.iter().for_each(|entry| {
        let rpm_file = std::fs::File::open(entry.path()).unwrap();
        let mut buf_reader = std::io::BufReader::new(rpm_file);
        let metadata = rpm::RPMPackageMetadata::parse(&mut buf_reader).unwrap();
        //src_rpm_map.entry(String::from(metadata.header.get_source_rpm().unwrap())).and_modify(|e| *e += 1).or_insert(1);
        let srcpkg_id = metadata.header.get_source_pkgid();
        metadata.header.get_source_rpm();
        //if srcpkg_id.is_err() && !metadata.header.get_name().unwrap().ends_with("-debuginfo") {
        //    println!("{:?} {:?}", metadata.header.get_name().unwrap(), srcpkg_id);
        //}

        let files = metadata.header.get_file_paths();
        if let Err(err) = metadata.header.get_payload_compressor() {
            //println!("error in {}: {:?}", metadata.header.get_name().unwrap(), err);
        } else {
            if let Ok(fff) = files {
                if metadata
                    .header
                    .get_name()
                    .unwrap()
                    .ends_with("-debugsource")
                {
                    file_count += fff.len();
                    if fff.len() > 10000 {
                        println!("{entry:?} {}", fff.len());
                    }
                }
                for fentry in metadata.header.get_file_entries().unwrap() {
                    if let FileMode::Symlink {} = fentry.mode {
                        //println!("{:?}", fentry);
                        let combined = fentry.path.parent().unwrap().join(fentry.linkto.clone());
                        let absolute = combined.absolutize();
                        // println!("aaaa {:?} {:?}", fentry.path, fentry.linkto);
                    }
                }
            }
        }

        //metadata.header.get_name();
        // println!("{:?}", metadata.header.get_name());
    });

    println!("Total RPM files: {i}");
    println!("Total files in debug source: {file_count}");
    // println!("RPM dict size = {}", src_rpm_map.keys().len());
    // println!("{:?}", src_rpm_map);
    */
}
