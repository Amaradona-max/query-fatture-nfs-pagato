from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import logging
import shutil
import uuid

from fastapi import APIRouter, Body, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

from app.core.config import settings
from app.services.file_processor import NFSFTFileProcessor, PisaFTFileProcessor, PisaRicevuteFTFileProcessor, CompareFTFileProcessor


router = APIRouter()
logger = logging.getLogger(__name__)
executor = ThreadPoolExecutor(max_workers=4)
tasks: dict[str, dict] = {}


def _ensure_dirs() -> None:
    settings.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    settings.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _run_single_file_task(task_id: str, processor, upload_path: Path, output_path: Path) -> None:
    tasks[task_id]["status"] = "processing"
    try:
        stats = processor.process_file(upload_path, output_path)
        tasks[task_id]["status"] = "done"
        tasks[task_id]["summary"] = stats
        tasks[task_id]["download_url"] = f"/api/download/{task_id}"
        upload_path.unlink(missing_ok=True)
    except Exception as exc:
        tasks[task_id]["status"] = "error"
        tasks[task_id]["error"] = str(exc)
        upload_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)


def _run_compare_task(
    task_id: str,
    upload_path_nfs: Path,
    upload_path_pisa: Path,
    output_path: Path,
) -> None:
    tasks[task_id]["status"] = "processing"
    try:
        summary = CompareFTFileProcessor().process_files(upload_path_nfs, upload_path_pisa, output_path)
        tasks[task_id]["status"] = "done"
        tasks[task_id]["summary"] = summary
        tasks[task_id]["download_url"] = f"/api/download/{task_id}"
        upload_path_nfs.unlink(missing_ok=True)
        upload_path_pisa.unlink(missing_ok=True)
    except Exception as exc:
        tasks[task_id]["status"] = "error"
        tasks[task_id]["error"] = str(exc)
        upload_path_nfs.unlink(missing_ok=True)
        upload_path_pisa.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)


@router.post("/process-file")
async def process_file(file: UploadFile = File(...)):
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Formato file non valido. Formati supportati: {', '.join(settings.ALLOWED_EXTENSIONS)}",
        )

    task_id = str(uuid.uuid4())
    upload_path = settings.UPLOAD_DIR / f"{task_id}_input{file_ext}"
    output_path = settings.OUTPUT_DIR / f"{task_id}_output.xlsx"

    try:
        _ensure_dirs()
        with upload_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        file_size = upload_path.stat().st_size
        if file_size > settings.MAX_FILE_SIZE:
            upload_path.unlink()
            raise HTTPException(
                status_code=400,
                detail=f"File troppo grande. Dimensione massima: {settings.MAX_FILE_SIZE / 1024 / 1024:.0f}MB",
            )

        tasks[task_id] = {"status": "queued", "file_id": task_id}
        executor.submit(_run_single_file_task, task_id, NFSFTFileProcessor(), upload_path, output_path)

        return {
            "success": True,
            "task_id": task_id,
        }
    except ValueError as exc:
        if upload_path.exists():
            upload_path.unlink()
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error("Errore elaborazione: %s", str(exc))
        if upload_path.exists():
            upload_path.unlink()
        if output_path.exists():
            output_path.unlink()
        raise HTTPException(status_code=500, detail="Errore durante l'elaborazione del file")


@router.post("/process-file-pisa")
async def process_file_pisa(file: UploadFile = File(...)):
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Formato file non valido. Formati supportati: {', '.join(settings.ALLOWED_EXTENSIONS)}",
        )

    task_id = str(uuid.uuid4())
    upload_path = settings.UPLOAD_DIR / f"{task_id}_input{file_ext}"
    output_path = settings.OUTPUT_DIR / f"{task_id}_output.xlsx"

    try:
        _ensure_dirs()
        with upload_path.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        file_size = upload_path.stat().st_size
        if file_size > settings.MAX_FILE_SIZE:
            upload_path.unlink()
            raise HTTPException(
                status_code=400,
                detail=f"File troppo grande. Dimensione massima: {settings.MAX_FILE_SIZE / 1024 / 1024:.0f}MB",
            )

        tasks[task_id] = {"status": "queued", "file_id": task_id}
        executor.submit(_run_single_file_task, task_id, PisaRicevuteFTFileProcessor(), upload_path, output_path)

        return {
            "success": True,
            "task_id": task_id,
        }
    except ValueError as exc:
        if upload_path.exists():
            upload_path.unlink()
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error("Errore elaborazione Pisa Ricevute: %s", str(exc))
        if upload_path.exists():
            upload_path.unlink()
        if output_path.exists():
            output_path.unlink()
        raise HTTPException(status_code=500, detail="Errore durante l'elaborazione del file")


@router.post("/process-compare")
async def process_compare(file_nfs: UploadFile = File(...), file_pisa: UploadFile = File(...)):
    file_ext_nfs = Path(file_nfs.filename).suffix.lower()
    file_ext_pisa = Path(file_pisa.filename).suffix.lower()
    if file_ext_nfs not in settings.ALLOWED_EXTENSIONS or file_ext_pisa not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Formato file non valido. Formati supportati: {', '.join(settings.ALLOWED_EXTENSIONS)}",
        )

    task_id = str(uuid.uuid4())
    upload_path_nfs = settings.UPLOAD_DIR / f"{task_id}_nfs_input{file_ext_nfs}"
    upload_path_pisa = settings.UPLOAD_DIR / f"{task_id}_pisa_input{file_ext_pisa}"
    output_path = settings.OUTPUT_DIR / f"{task_id}_output.xlsx"

    try:
        _ensure_dirs()
        with upload_path_nfs.open("wb") as buffer:
            shutil.copyfileobj(file_nfs.file, buffer)
        with upload_path_pisa.open("wb") as buffer:
            shutil.copyfileobj(file_pisa.file, buffer)

        file_size_nfs = upload_path_nfs.stat().st_size
        file_size_pisa = upload_path_pisa.stat().st_size
        if file_size_nfs > settings.MAX_FILE_SIZE or file_size_pisa > settings.MAX_FILE_SIZE:
            if upload_path_nfs.exists():
                upload_path_nfs.unlink()
            if upload_path_pisa.exists():
                upload_path_pisa.unlink()
            raise HTTPException(
                status_code=400,
                detail=f"File troppo grande. Dimensione massima: {settings.MAX_FILE_SIZE / 1024 / 1024:.0f}MB",
            )

        tasks[task_id] = {"status": "queued", "file_id": task_id}
        executor.submit(_run_compare_task, task_id, upload_path_nfs, upload_path_pisa, output_path)

        return {
            "success": True,
            "task_id": task_id,
        }
    except ValueError as exc:
        if upload_path_nfs.exists():
            upload_path_nfs.unlink()
        if upload_path_pisa.exists():
            upload_path_pisa.unlink()
        if output_path.exists():
            output_path.unlink()
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error("Errore confronto file: %s", str(exc))
        if upload_path_nfs.exists():
            upload_path_nfs.unlink()
        if upload_path_pisa.exists():
            upload_path_pisa.unlink()
        if output_path.exists():
            output_path.unlink()
        raise HTTPException(status_code=500, detail="Errore durante il confronto dei file")


@router.get("/task/{task_id}")
async def get_task_status(task_id: str):
    task = tasks.get(task_id)
    if not task:
        output_path = settings.OUTPUT_DIR / f"{task_id}_output.xlsx"
        if output_path.exists():
            return {
                "status": "done",
                "file_id": task_id,
                "download_url": f"/api/download/{task_id}",
            }

        artifacts = list(settings.UPLOAD_DIR.glob(f"{task_id}*"))
        if artifacts:
            latest_mtime = max(p.stat().st_mtime for p in artifacts)
            if datetime.now().timestamp() - latest_mtime > 20 * 60:
                return {
                    "status": "error",
                    "file_id": task_id,
                    "error": "Elaborazione scaduta. Ricarica il file e riprova.",
                }
            return {
                "status": "processing",
                "file_id": task_id,
            }

        raise HTTPException(status_code=404, detail="Task non trovato")
    return task


@router.post("/close-day")
async def close_day(payload: dict = Body(...)):
    message = str(payload.get("message", "")).strip()
    if "saluti fine giornata" not in message.lower():
        raise HTTPException(status_code=400, detail="Messaggio di chiusura non valido")

    riepilogo_path = settings.BASE_DIR.parent / "Riepilogo_Istruzioni_App.md"
    if not riepilogo_path.exists():
        raise HTTPException(status_code=404, detail="Riepilogo istruzioni non trovato")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    entry = f"\n## Chiusura giornata {timestamp}\n- {message}\n- Riepilogo aggiornato automaticamente.\n"
    content = riepilogo_path.read_text(encoding="utf-8")
    riepilogo_path.write_text(content + entry, encoding="utf-8")

    return {"success": True, "timestamp": timestamp}


@router.get("/download/{file_id}")
async def download_file(file_id: str):
    output_path = settings.OUTPUT_DIR / f"{file_id}_output.xlsx"

    if not output_path.exists():
        raise HTTPException(status_code=404, detail="File non trovato o scaduto")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"File_Riepilogativo_NFS_FT_{timestamp}.xlsx"

    return FileResponse(
        path=output_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "NFS/FT File Processor"}
