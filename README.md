# Google-File-System
Python implementation of [Google File System](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf).

## Compile
- Run `bash clean.sh`

## Dependency
- gRPC

## Run
- In one terminal, run `python chunkServer.py` to start chunk servers.
- In other terminal, run `python masterServer.py` to start master server.
- From the third terminal, run `python client.py` to and type in any of the provided commands to interact with the file system.

### Commands
- `create <file_path>` : Creates a new file with given absolute file path `<file_path>`
- `list <prefix>` : Lists all files whose absolute path have prefix `<prefix>`
- `append <file_path> <string>` : Appends `<string>` to file `<file_path>`
- `pead <file_path> [offset] [len]` : Reads `[len]` characters of file `<file_path>` starting from `[offset]`. Default value of `offset` is 0 and that of `len` is EOF.
- `delete <file_path>` : delete the file with given absolute file path `<file_path>`