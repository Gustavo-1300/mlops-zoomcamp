import os
import sys
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from week_3.orchestrate import main_flow

deployment = Deployment.build_from_flow(
    flow=main_flow,
    name="first-deployment", 
    version=1, 
    work_pool_name="zoom-pool",
    schedule=CronSchedule(cron="0 9 1 12 *", timezone="UTC")
)

if __name__ == "__main__":
    deployment.apply()