python air_quality_flow.py \
--input gs://airqualitylcsqa \
--output covid-1:airquality.E2 \
--project covid-1 \
--job_name airqualityflow \
--region europe-west1 \
--save_main_session \
--staging_location gs://airqualitylcsqa/temp/ \
--temp_location gs://airqualitylcsqa/temp \
--runner DataflowRunner