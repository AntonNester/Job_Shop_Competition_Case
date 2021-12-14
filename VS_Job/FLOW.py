# import numpy as np
# import pandas as pd

import DATA_LOAD as dl
import INITIALIZATION as ini
import JOB_SHOP as js

# file = 'competition_case.xlsx'
# file = 'test_case.xlsx'
file = 'test_case_new.xlsx'

#   Загрузка данных из файла
with dl.load_from_file(file) as db:
    equipment = db.load_equipment()  # Список оборудования
    subproduct = db.load_subproduct() # Связка продукта и оборудования
    switch_time = db.load_switch_time() # Время на переоснастку
    structure = db.load_structure() # Общая структура заказов
    order_graph = db.load_order_graph() # Precedence constraint
    movement_time = db.load_movement_time() # Время на перемещение между оборудованием
    
    orders = db.load_orders() # Цены и финальные продукты

#   Предварительная обработка данных
with ini.preprocessing_data(subproduct,structure,equipment,switch_time,order_graph,movement_time) as preData:
    interval_data = preData.pre_interval_data() # Комбинированные данные для интервальной переменной
    imply_data = preData.imply_data() # Данные для последовательности выполнения операций
    start_stop_data = preData.start_stop_data() # Интервалы требующие время на перемещение между оборудованием


#   Расчет модели
problem = js.job_shop_model(orders,interval_data,imply_data,start_stop_data,file)
problem.solve()

#   Выгрузка результатов
solution = problem.output_data()
solution.to_csv('solution.csv', sep='\t', encoding='utf-8')