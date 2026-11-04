"""Ejecuta el entregable completo y guarda sus evidencias incluso si falla una celda."""
from pathlib import Path
import nbformat
from nbclient import NotebookClient
from .config import ROOT
from .start import initialize


def main():
    initialize()
    notebook=nbformat.read(ROOT/'EP3_DataRetail360.ipynb',as_version=4)
    output=ROOT/'artifacts'/'EP3_DataRetail360.ejecutado.ipynb'
    output.parent.mkdir(parents=True,exist_ok=True)
    try:
        NotebookClient(notebook,timeout=3600,kernel_name='python3',
            resources={'metadata':{'path':str(ROOT)}}).execute()
    finally:
        nbformat.write(notebook,output)
    print(f'Notebook completo verificado: {output}')


if __name__=='__main__':
    main()
