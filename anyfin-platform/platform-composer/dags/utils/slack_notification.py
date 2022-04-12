from random import choice
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


def task_fail_slack_alert(slack_connection, context):
    emoji_list = [':smiling_face_with_tear:', ':shushing_face:', ':face_with_raised_eyebrow:', ':pleading_face:',
                  ':see_no_evil:', ':sadblob:', ':meow_headache:', ':nuke:', ':this-is-fine-fire:', ':crazy_cowboy:',
                  ':hmm_parrot:']
    slack_webhook_token = BaseHook.get_connection(slack_connection).password
    slack_msg = """
            {emoji} Task Failed
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}
            *Log*: {log_url}
            """.format(
            emoji=choice(emoji_list),
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )

    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=slack_connection,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)
