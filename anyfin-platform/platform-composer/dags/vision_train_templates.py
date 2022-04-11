from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.gcp_compute_operator import GceInstanceStartOperator, GceInstanceStopOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

SETTINGS = Variable.get("vision_secret", deserialize_json=True)

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

SE_LENDERS_BY_WEEKDAY = {
    # Monday
    0: ['"American Express"', '"Bank Norwegian"', '"Credway"', '"Monetti"', '"DNB"', '"Komplett.se"'],
    # Tuesday
    1: ['"Ferratum Bank"', '"Ica Banken"', '"Ikea"', '"Everyday+"', '"Ecster"', '"Euroloan"'],
    # Wednesday
    2: ['"Klarna"', '"Nordea"', '"Re:member"', '"Marginalen Bank"', '"Agriakortet"', '"Coop"'],
    # Thursday
    3: ['"SEB"', '"Svea Ekonomi"', '"Swedbank"', '"MoneyGo"', '"Avida Finans AB"', '"Everydaycard"'],
    # Friday
    4: ['"Wasa Kredit"', '"Qliro"', '"Bubbleroom"', '"Lendify"', '"Lendify"'],
    # Saturday
    5: ['"Ellos"', '"Jotex"', '"Santander"', '"Resurs Bank"', '"Collector Bank"', '"NetonNet"'],
    # Sunday
    6: ['"Volvofinans"', '"Ikano Bank"', '"H&M"', '"Easycredit"', '"Credigo"', '"Okq8"']
}

FI_LENDERS_BY_WEEKDAY = {
    # Monday
    0: ['"Klarna"', '"Qliro"', '"Santander"'],
    # Tuesday
    1: ['"Bank Norwegian"', '"Ellos"', '"Nordea Credit"'],
    # Wednesday
    2: ['"Ferratum"', '"Saldolimiitti"', '"Svea Ekonomi"'],
    # Thrusday
    3: ['"IPF Digital"', '"Resurs Bank"', '"Instabank"', '"Bubbleroom"'],
    # Friday
    4: ['"Blue finance"', '"Testlender"', '"Komplett Bank"'],
    # Saturday
    5: ['"Collector Bank"', '"Credigo"', '"Suomilimiitti"'],
    # Sunday
    6: ['"Fellow Finance"', '"Vippi"', '"GF Money Consumer Finance"'],
}

COUNTRY_SPECIFICS = {
    'SE': {'lenders': SE_LENDERS_BY_WEEKDAY, 'country_code': 'SE'},
    'FI': {'lenders': FI_LENDERS_BY_WEEKDAY, 'country_code': 'FI'}
}


FETCHING_REPO = f"""
    gcloud beta compute ssh --zone "europe-west1-b" "vision-train" --project "anyfin-platform" -- \
    'sudo apt update;' \
    'sudo apt install -y poppler-utils;' \
    'sudo apt install -y python3-pip;' \
    'git clone https://{SETTINGS['login']}:{SETTINGS['token']}@github.com/anyfin/vision-service.git;' \
    'cd vision-service;' \
    'git checkout updates || git checkout -b updates;' \
    'gsutil cp gs://anyfin-vision/.env .;' \
    'sudo pip3 install -r requirements.txt;'
"""

COMMIT_REPO = f"""
    gcloud beta compute ssh --zone "europe-west1-b" "vision-train" --project "anyfin-platform" -- \
    'cd vision-service;' \
    'git checkout updates || git checkout -b updates;' \
    'git commit -m "Template updates `date +\"%Y-%m-%d %H:%M:%S\"`";' \
    'git push origin updates;' \
    'cd ..;' \
    'rm -rf vision-service;'
"""


# returns the week day (monday, tuesday, etc.)
def get_day(**kwargs):
    kwargs['ti'].xcom_push(key='day', value=kwargs['execution_date'].day_of_week)


# returns the name id of the task to launch prefixed with the market country code
# (e.g. SE_templates_train_for_monday, FI_templates_train_for_tuesday, etc.)
def branch(country_code, **kwargs):
    return f"{country_code}_templates_train_for_" + WEEKDAYS[kwargs['ti'].xcom_pull(task_ids='weekday', key='day')]


default_args = {
    'owner': 'de-anyfin',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'email': Variable.get('ds_email'),
    'start_date': datetime(2020, 5, 10),
    'depends_on_past': False
}

with DAG(
        'vision_train_templates',
        default_args=default_args,
        catchup=False,
        schedule_interval='@daily',
        max_active_runs=1
) as dag:

    gce_instance_start = GceInstanceStartOperator(
        project_id='anyfin-platform',
        zone='europe-west1-b',
        resource_id='vision-train',
        task_id='gcp_compute_start_task',
        dag=dag
    )

    gce_instance_stop = GceInstanceStopOperator(
        project_id='anyfin-platform',
        zone='europe-west1-b',
        resource_id='vision-train',
        task_id='gcp_compute_stop_task',
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    get_weekday = PythonOperator(
        task_id='weekday',
        python_callable=get_day,
        provide_context=True,
        dag=dag
    )

    clone_repo = BashOperator(
        task_id="fetching_vision_repo",
        bash_command=FETCHING_REPO,
        dag=dag
    )

    commit_repo = BashOperator(
        task_id="update_vision_repo",
        bash_command=COMMIT_REPO,
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag
    )

    # task 1, get the week day
    gce_instance_start.set_downstream(get_weekday)
    get_weekday.set_downstream(clone_repo)

    # One bash operator for each week day, all branched depending on weekday
    for country_key, country_values in COUNTRY_SPECIFICS.items():

        # BranchPythonOperator will use "weekday" variable, and decide which task to launch next
        branching = BranchPythonOperator(
            task_id=f"{country_values['country_code']}_branching",
            python_callable=branch,
            op_kwargs={
                'country_code': country_values['country_code']
            },
            provide_context=True,
            dag=dag)

        clone_repo.set_downstream(branching)

        for day in range(0, 7):
            train_templates = f"""
                gcloud beta compute ssh --zone "europe-west1-b" "vision-train" --project "anyfin-platform" -- \
                'cd vision-service;' \
                'git checkout updates || git checkout -b updates;' \
                'python3 -m src.vision_service --log=INFO -a train -n 50 -c {country_values['country_code']} -l {' '.join(country_values['lenders'][day])};' \
                'git config --local user.email "charalampos@anyfin.com";' \
                'git config --local user.name "vision-train";' \
                'git pull origin updates;' \
                'git add src/knowledge_base_templates/{country_values['country_code']}/*;'
                """

            bash_operator = BashOperator(
                task_id=f"{country_values['country_code']}_templates_train_for_" + WEEKDAYS[day],
                bash_command=train_templates,
                dag=dag
            )
            bash_operator.set_upstream(branching)
            bash_operator.set_downstream(commit_repo)

    commit_repo.set_downstream(gce_instance_stop)
