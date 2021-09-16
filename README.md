# Google-File-System
Python implementation of [Google File System](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf).

## Compile
- Run `bash clean.sh`

## Dependency
- gRPC

## Run
- Open three terminals, and run `chunkServer.py`, `masterServer.py` and `client.py` in each of them.
- Type in the supported commands in the terminal which is running `client.py`.

### Commands
- `create <file_path>` : Creates a new file with given absolute file path `<file_path>`
- `list <prefix>` : Lists all files whose absolute path have prefix `<prefix>`
- `append <file_path> <string>` : Appends `<string>` to file `<file_path>`
- `read <file_path> [offset] [len]` : Reads `[len]` characters of file `<file_path>` starting from `[offset]`. Default value of `offset` is 0 and that of `len` is -1(EOF).
- `delete <file_path>` : delete the file with given absolute file path `<file_path>`