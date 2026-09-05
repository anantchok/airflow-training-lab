from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils import timezone


INPUT_DIR = Path("/opt/airflow/data/input")

REQUIRED_FILES = [
    "1.Doctor.xlsb",
    "2.Nurse.xlsb",
    "3.Pharmacist.xlsb",
    "4.Dietitian.xlsb",
    "5.Physiotherapist.xlsb",
    "6.CSR.xlsb",
    "7.Lab.xlsb",
    "8.X-Ray.xlsb",
    "ResultMRauditJCIHA.xlsx",
]


@dag(
    dag_id="mra_01_check_files",
    description="ตรวจสอบไฟล์ต้นทางสำหรับ MRA ETL",
    schedule=None,
    start_date=timezone.datetime(2026, 1, 1),
    catchup=False,
    tags=["mra", "etl", "data-quality"],
)
def mra_check_files():

    @task
    def check_input_files():
        missing_files = []
        results = []

        for filename in REQUIRED_FILES:
            file_path = INPUT_DIR / filename

            if not file_path.exists():
                missing_files.append(filename)
                continue

            results.append(
                {
                    "filename": filename,
                    "size_bytes": file_path.stat().st_size,
                    "status": "FOUND",
                }
            )

        if missing_files:
            raise FileNotFoundError(
                "ไม่พบไฟล์: " + ", ".join(missing_files)
            )

        print("ตรวจพบไฟล์ครบทั้งหมด 9 ไฟล์")

        for result in results:
            print(
                result["filename"],
                result["size_bytes"],
                "bytes",
            )

        return {
            "status": "READY",
            "files_found": len(results),
        }

    check_input_files()


mra_check_files()