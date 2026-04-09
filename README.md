# DICOM Cleaner for Win7 32-bit

This tool removes the DICOM tag `(0008,0005) Specific Character Set` from files under a target folder.

## What it does

- Recursively scans a target folder
- Supports incremental mode
- Only removes `(0008,0005)`
- Keeps all other bytes as-is whenever possible
- Builds a `windows/386` executable for Windows 7 32-bit

## Main files

- `cmd/dicomcleaner/main.go` : main program
- `run_cleaner_full.bat` : Windows helper script with log output
- `run_cleaner_simple.bat` : minimal Windows helper script
- `.github/workflows/build-win7-386.yml` : GitHub Actions workflow

## Build on macOS

```bash
./build_windows_386.sh
```

## Build on GitHub Actions

Push this repository to GitHub, then run:

- `Actions`
- `Build Win7 32-bit EXE`

The workflow uploads:

- `dicom_cleaner.exe`
- `run_cleaner_full.bat`
- `run_cleaner_simple.bat`

## Windows usage

Double-click:

- `run_cleaner_full.bat`

Or run from command line:

```bat
run_cleaner_full.bat E:\DICOM
```

## Notes

- The program uses incremental scanning, which means it still walks the directory tree but only processes new or changed files.
- The current implementation requires a standard `DICM` preamble.
- If a file does not contain `(0008,0005)`, it will be left unchanged.
