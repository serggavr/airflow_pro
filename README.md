### Задание ПРО
- Создайте переменную (Admin->Variables), в которой пропишите путь к raw_data
- Создайте connection (Admin->connections), в котором пропишите настройки соединения к БД raw_store
- В новом созданном DAGe:
    - Создайте пустой таск, используя DammyOperator, назовите его “start”
    - Далее, используя BashOperator, создайте таск, который создаст папку /raw_data/supermarket_1
    - Далее, используя PythonOperator создайте таск, который загружает данные из csv файла (Yandex в raw_store как есть.

    При выполнении данного пункта используйте созданное соединение (connection)
Дополните ваш DAG, написав Sendor, который проверяет наличие файла в папке /raw_data/supermarket_1
       Загрузку данных на шаге 5. Осуществлять, если файл существует

 

#### Задание 2 (формирование CORE)

- Напишите таск или группу тасков, которые формируют CORE-слой
    CORE слой должен быть нормализован. (архитектуру CORE выбирайте сами)

    Примечание: формирование CORE-слоя можно сделать на этапе развертывания airflow (в докер-compose) при помощи скриптов

- Далее, напишите таск, который реализует следующую логику:

   - a) Ваши загруженные данные содержат столбцы Order Date и Ship Date с датами в виде MM/DD/YYYY
   - b) Ваша задача преобразовать данные в указанных столбцах к виду YYYY-MM-DD
   - c) Отфильтровать исходный источник данных и выбрать данные, относящиеся только к корпоративному сегменту  
   - d) Полученную отфильтрованную выборку записать базу core_store

    Варианты реализации: (можете выбрать любой)
     1. Используйте PythonOperator, а также библиотеку psycopg2 для работы с БД Postgres
     2. Используйте PostgresOperator

 

#### Задание 3. Формирование Data MART

Для отчетности, аналитики запросили следящие данные.

 

- Витрина 1  
Вывести по каждому году, общую сумму заказов по категориям корпоративного сегмента.
Данную витрину записать в mart_store

- Витрина 2  
Используя PythonOperator напишите task, который случайным образом возвращает одно из возможных значений атрибута "Category" в наборе данных о заказах супермаркета.

- Использовать механизм XCOM для передачи полученного значения Category (п.1) следующей задаче (см.п.3)

- Напишите task, который реализует следующую логику: в зависимости от возвращенного значения Category найдите итоговую сумму заказов по каждой из суб-категорий выбранной категории в корпоративном сегменте за 2015 год. Результат вычислений запишите в mart_store

     Для выполнения данного пункта, использовать BranchPythonOperator 

- Далее, Создайте пустой таск, используя DammyOperator, назовите его “end”

В результате выполнения задания необходимо предоставить:  
Исходный код DAGa  
Скриншот построенного DAGa в Airflow  
Скриншот витрины mart_store
Результат выполнения задания необходимо выложить в github/gitlab и указать ссылку на Ваш репозиторий (не забудьте — репозиторий должен быть публичным).

***

### Решение

Запуск: `docker-compose up`

Код DAG'a `airflow/dags/supermarket.py`

Схема CORE слоя:  
![core_store](./core_store.png)  

Mart Витрина 1:  
![Витрина 1](./mart_1.png)

Mart Витрина 2:  
![Витрина 1](./mart_2.png)

Airflow connections:  
![Airflow connections](./airflow_connections.png)  

Airflow variables:  
![Airflow variables](./airflow_variables.png)

DAG graph:  
![Airflow variables](./dag_graph.png)



