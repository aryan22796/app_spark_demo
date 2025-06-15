from airflow.models.dagbag import DagBag

def test_dags_import_without_errors():
    dag_bag = DagBag()
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"