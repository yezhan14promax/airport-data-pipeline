# Airport

这个项目是一个机场数据管道，利用 Apache Spark 和 Apache Airflow 进行数据处理和编排。
它旨在提供一个可伸缩且高效的解决方案，用于分析和管理与机场相关的数据。

## 安装

首先，你需要安装 Spark 和 Airflow。你可以参考以下链接进行安装：

- [Spark 安装指南](https://spark.apache.org/docs/latest/)
- [Airflow 安装指南](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

注意：默认情况下，如果您未指定 AIRFLOW_HOME 环境变量，Airflow 将使用 $HOME/airflow 文件夹作为其配置、DAG 和元数据的默认位置。

  如果您指定了 AIRFLOW_HOME，Airflow 将使用您指定的目录。 这允许您根据需要自定义 Airflow 存储其配置文件、DAG 和元数据的位置。
  


```bash
export AIRFLOW_HOME=/"path to your workspace"/
```


然后，你需要安装 Python 的 datatime 和 subprocess 库。你可以使用 pip 进行安装：

```python
pip install datetime
pip install subprocess
```

## 使用

在安装了所有依赖之后，你可以运行`dag.py`脚本来开始你的数据处理任务：

```bash
python3 ./airflow/dags/dag.py
```

## 脚本说明

- `recuperer.py`：这个脚本用于获取数据。
- `analyse.py`：这个脚本用于数据分析。
- `nettoyage.py`：这个脚本用于清洗数据。
- `formation.py`：这个脚本用于数据预处理。
- `evaluation.py`：这个脚本用于评估模型。


## 贡献

欢迎任何形式的贡献，包括但不限于问题报告、功能请求、代码提交。

## 许可

本项目使用 MIT 许可证，详情请参见[LICENSE](LICENSE)文件。
