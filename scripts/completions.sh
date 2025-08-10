#!/usr/bin/env bash

rm -rf completions
mkdir -p completions
go run . completion bash > completions/grapple.bash
go run . completion zsh > completions/grapple.zsh
go run . completion fish > completions/grapple.fish
go run . completion powershell > completions/grapple.ps1
