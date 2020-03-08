#!/usr/bin/env bash
venv_dir=${1:-.venv}

python3 -m venv "${venv_dir%%/}"
echo -e "call this:\n. \"${venv_dir%%/}/bin/activate\""
