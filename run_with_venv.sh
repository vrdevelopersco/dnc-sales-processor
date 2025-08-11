#!/bin/bash
cd /media/bodega/procesador
export PATH="/media/bodega/procesador/bin:$PATH"
export PYTHONPATH="/media/bodega/procesador/lib/python3.12/site-packages:$PYTHONPATH"
/media/bodega/procesador/bin/python "$@"
