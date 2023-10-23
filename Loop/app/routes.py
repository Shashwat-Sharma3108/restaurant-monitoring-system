import secrets
from fastapi import APIRouter, BackgroundTasks, Request, status
from fastapi.responses import FileResponse

from datetime import datetime
from loguru import logger

from models import Report
from database import get_db
from reports import create_report

router = APIRouter(tags=["restaurant-monitoring-system"])

@router.get('/get_report')
def get_report(report_id: str,request: Request):
    if report_id:
        with get_db() as db:
            data = db.query(Report).filter(
                Report.report_id == report_id
            ).first()

        if data:
            if data.status == 'ERROR':
                return {'status': status.HTTP_503_SERVICE_UNAVAILABLE, 'message':'We are working on it, Please try again later'}
            if data.status == 'FINISHED':
                return FileResponse(path=f'../Output_CSV/{report_id}.csv',filename='output.csv')
            if data.status == 'PENDING':
                return {'status': status.HTTP_204_NO_CONTENT, 'message':'Your file is processing please wait!'}
        else:
            return {'status':status.HTTP_400_BAD_REQUEST, 'message':f"No reports found for {report_id}"}
    else:
        return {'status':status.HTTP_400_BAD_REQUEST, 'message':f"No reports found for {report_id}"}

@router.post('/trigger_report')
def trigger_report(request: Request, background_tasks: BackgroundTasks):
    try:
        report_id = secrets.token_urlsafe(16)
        new_report = Report(
            report_id=report_id,
            status = 'PENDING',
            created_at = datetime.now()
        )

        background_tasks.add_task(create_report,report_id)

        with get_db() as db:
            db.add(new_report)
            db.commit()

        return {'status':status.HTTP_201_CREATED, 'message':'File is processing in the background', 'data':report_id}
    except Exception as e: 
        logger.error(f"ERROR : {e}")
        return {'status':status.HTTP_500_INTERNAL_SERVER_ERROR, 'message':f"ERROR : {e}"}