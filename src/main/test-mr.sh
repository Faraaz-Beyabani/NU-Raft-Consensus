#!/bin/bash
here=$(dirname "$0")
[[ "$here" = /* ]] || here="$PWD/$here"
export GOPATH="$here/../../"
echo ""
echo "==> Part 1"
go test -run Sequential mapreduce/...
echo ""
echo "==> Part 1B"
(cd "$here" && sh ./test-wc.sh > /dev/null)
echo ""
echo "==> Part 2"
go test -run TestParallel mapreduce/...
echo ""
echo "==> Part 2B"
go test -run Failure mapreduce/...
echo ""

rm "$here"/mrtmp.* "$here"/diff.out
