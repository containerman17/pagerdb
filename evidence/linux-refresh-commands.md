Linux verification setup, 2026-10-14 JST

Created only these machines. Existing ubuntu preserved:

    orb create --arch arm64 ubuntu:24.04 pagerdb-arm64
    orb create --arch amd64 ubuntu:24.04 pagerdb-amd64

For each machine:

    orb -m MACHINE sudo -n apt-get update
    orb -m MACHINE sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y gcc libc6-dev
    orb -m MACHINE sudo -n sysctl -w vm.unprivileged_userfaultfd=1
    orb -m MACHINE mkdir -p /home/ilia/pagerdb/.scratch/tmp
    orb -m MACHINE getconf PAGESIZE

Go 1.26.0 archives downloaded from https://go.dev/dl/go1.26.0.linux-arm64.tar.gz and https://go.dev/dl/go1.26.0.linux-amd64.tar.gz, extracted under .scratch/linux-ARCH.

Arm64 command prefix:

    orb -m pagerdb-arm64 env CGO_ENABLED=1 PAGERDB_REQUIRE_UFFD=1 TMPDIR=/home/ilia/pagerdb/.scratch/tmp GOCACHE=/Users/ilia/pagerdb/.scratch/cache-arm64 GOMODCACHE=/Users/ilia/pagerdb/.scratch/mod-arm64 /Users/ilia/pagerdb/.scratch/linux-arm64/go/bin/go

Amd64 command prefix:

    orb -m pagerdb-amd64 env CGO_ENABLED=1 PAGERDB_REQUIRE_UFFD=1 GODEBUG=asyncpreemptoff=1 TMPDIR=/home/ilia/pagerdb/.scratch/tmp GOCACHE=/Users/ilia/pagerdb/.scratch/cache-amd64 GOMODCACHE=/Users/ilia/pagerdb/.scratch/mod-amd64 /Users/ilia/pagerdb/.scratch/linux-amd64/go/bin/go

The amd64 setting avoids a previously observed Go compiler/runtime issue under translation. Both machines report OS page size4096. No tests or benchmarks started during setup.
