from fastapi import FastAPI, status

from routes import router as restaurant_router

app = FastAPI(title='Restaurant Monitoring System')

app.include_router(restaurant_router, prefix='/api/v1')

@app.get("/ah/_health")
def health():
    return {'status':status.HTTP_200_OK,'message':'Working Fine'}