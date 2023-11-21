use core::time;
use std::{collections::HashMap, fs, thread::sleep};

use path_absolutize::*;

use rpm;
use rpm::FileMode;

fn main() {
    let folder =
        fs::read_dir("/home/marxin/Data/ftp.sh.cvut.cz/opensuse/tumbleweed/repo/oss/x86_64");
    let folder2 =
        fs::read_dir("/home/marxin/Data/ftp.gwdg.de/pub/opensuse/debug/tumbleweed/repo/oss/x86_64");

    //let mut src_rpm_map = HashMap::new();
    //let mut seen_paths = Vec::new();

    let mut file_count = 0;
    let mut i = 0;
    let mut entries = Vec::new();

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
}
