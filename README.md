# debuginfod-rs

An extremely fast [debuginfod](https://sourceware.org/elfutils/Debuginfod.html) server, written in Rust.

```
[2023-12-13T08:35:11.001Z INFO  debuginfod_rs] walking 173017 RPM files (477.8 GB)
[2023-12-13T08:35:12.389Z INFO  debuginfod_rs] parsing took: 2.09 s (228.8 GB/s)
```

- ⚡️ ~30x faster than the elfutils' debuginfod (only RPM metadata are parsed)
- 🧵 multithreaded parser and web server
- 🦋 in-memory database (~200MiB per 1TB of the indexed RPM files)
- 📦 RPM-based only (openSUSE and Fedora/RHEL packages supported)
- 🌐 full debuginfod Web API supported
- 🗜 commonly used compressions supported (bzip2, gzip, xz, zstd)

## Example usage

![debuginfod demo example](docs/demo.gif).
