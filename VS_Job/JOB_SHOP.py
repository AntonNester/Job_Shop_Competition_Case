# Общая постановка
# Есть предприятие, на котором производится продукция различного ассортимента. 
# На производство поступают заказы от клиентов на изготовление определенного типа и объема продукции. 
# Предполагается, что портфель заказов и их стоимость известны перед началом планирования. 
# Каждый заказ имеет свою технологическую карту производства, т.е. последовательность операций от сырья до 
# получения готовой продукции. Операции по обработке материала выполняются на оборудовании, которое предназначено 
# для выполнения определенного типа операций (может быть указано несколько типов операций для одного оборудования). 
# Промежуточный продукт производственной цепи называется полуфабрикатом. Прежде чем приступить к следующей 
# операции на оборудовании необходимо произвести операцию переналадки оборудования 
# (переключения оборудования на другую операцию, подготовка к обработке нового полуфабриката, очистка оборудования и т.д.).
#  Кроме того, необходимо учитывать время перемещения полуфабрикатов продукции между цехами. 

# Цель решения задачи
# Построить расписание производства заказов таким образом, 
# чтобы максимизировать выручку производства на фиксированном промежутке времени (30 дней). 

# Ограничения
# 1.	Если режим работы оборудования соответствует mode_0, то одновременно на этом оборудовании 
#       может выполнятся только одна операция;
# 2.	Перед каждой операцией по обработке полуфабрикатов необходимо произвести переналадку;
# 3.	Операции переналадки и обработки полуфабриката не могут происходить одновременно;
# 4.	Заказ может состоять из нескольких конечных продуктов. Частичное выполнение заказа к отчетной дате 
#       добавляет 0 ед. к выручке;
# 5.	Каждый конечный продукт в заказе имеет последовательность технологических операций, которую нельзя нарушать; 
# 6.	Перемещение, переналадка и обработка полуфабриката не могут выполняться одновременно для одного полуфабриката.

# Допущения
# 1.	При планировании не учитываются потери сырья в процессе производства. Таким образом, масса готовой 
#       продукции равна массе исходного сырья. 
# 2.	Оборудование работает без перерывов. Таким образом, технологические перерывы и регламентные 
#       процедуры не учитываются при планировании.
# 3.	Не все заказы должны быть запланированы. 

import collections
from ortools.sat.python import cp_model
import pandas as pd

class job_shop_model():

    def __init__(self, orders,interval_data,imply_data,start_stop_data,file):
        self.file = file
        #   Горизонт планирования
        self.horizon = 60*24*30
        #   Интервальные переменные
        # **********************************************************************
        # item[0]   ->  'ORDER_ID'
        # item[1]   ->  'SUBORDER_ID' 
        # item[2]   ->  'SUBPRODUCT_ID' 
        # item[3]   ->  'EQUIPMENT_ID' 
        # item[4]   ->  'MAKE_TIME'
        # item[5]   ->  'UNIT' 
        # item[6]   ->  'EQUIPMENT_MODE' 
        # item[7]   ->  'SETUP_TIME'
        self.interval_data = [(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7])
                      for item in interval_data.itertuples(index=False)]

        #   Последовательность операций
        # **********************************************************************
        # item[0]   ->  'FROM_ORDER_ID'
        # item[1]   ->  'FROM_SUBORDER_ID'
        # item[2]   ->  'FROM_SUBPRODUCT_ID',
        # item[3]   ->  'TO_ORDER_ID'
        # item[4]   ->  'TO_SUBORDER_ID'
        # item[5]   ->  'TO_SUBPRODUCT_ID'
        self.imply_data = [(item[0], item[1], item[2], item[3], item[4], item[5])
                      for item in imply_data.itertuples(index=False)]

        #   Структура заказов
        # **********************************************************************
        # item[0]   ->  'ORDER_ID'
        # item[1]   ->  'SUBORDER_ID' 
        # item[2]   ->  'SUBPRODUCT_ID' 
        col_data = interval_data[['ORDER_ID','SUBORDER_ID','SUBPRODUCT_ID']].drop_duplicates()

        self.structure = [(item[0], item[1], item[2])
                      for item in col_data.itertuples(index=False)]

        #   Список заказов
        # **********************************************************************         
        self.all_orders = interval_data['ORDER_ID'].unique()        

        #   Список подзаказов
        # **********************************************************************        
        self.all_suborders = interval_data['SUBORDER_ID'].unique()

        #   Список оборудования
        # **********************************************************************
        # item[0]   ->  'EQUIPMENT_ID'
        self.all_machines = interval_data['EQUIPMENT_ID'].unique()

        #   Время на перемещение между оборудованием
        # **********************************************************************
        # item[0]   ->  'FROM_ORDER_ID'
        # item[1]   ->  'FROM_SUBORDER_ID'
        # item[2]   ->  'FROM_SUBPRODUCT_ID'
        # item[3]   ->  'FROM_EQUIPMENT_ID'
        # item[4]   ->  'TO_ORDER_ID'
        # item[5]   ->  'TO_SUBORDER_ID'
        # item[6]   ->  'TO_SUBPRODUCT_ID'
        # item[7]   ->  'TO_EQUIPMENT_ID'
        # item[8]   ->  'MOVE_TIME'
        self.start_stop_data = [(item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8])
                      for item in start_stop_data.itertuples(index=False)]

        #   Цены и финальные продукты
        # **********************************************************************
        # item[0]   ->  'ORDER_ID'
        # item[1]   ->  'QNT'
        # item[2]   ->  'UNIT'
        # item[3]   ->  'PRICE'
        # item[4]   ->  'FIN_SUBORDER_ID' 
        self.orders = [(item[0], item[1], item[2], item[3], item[4])
                      for item in orders.itertuples(index=False)]

        self.createModel()


    def createModel(self):
        self.model = cp_model.CpModel()

        task_type = collections.namedtuple('task_type', 'start end present interval')
        self.all_tasks = {}
        task_start = collections.defaultdict(list)
        task_end = collections.defaultdict(list)
        task_present =  collections.defaultdict(list)
        equip_present = collections.defaultdict(list)  
        machine_to_intervals = collections.defaultdict(list)

        task_start_move = collections.defaultdict(list)
        task_end_move = collections.defaultdict(list)
        task_present_move = collections.defaultdict(list)

        #   Создаем переменные для расчета
        for item in self.interval_data:
            ORDER_ID        =   item[0]
            SUBORDER_ID     =   item[1]
            SUBPRODUCT_ID   =   item[2]
            EQUIPMENT_ID    =   item[3] 
            MAKE_TIME       =   item[4]
            UNIT            =   item[5]
            EQUIPMENT_MODE  =   item[6]
            SETUP_TIME      =   item[7]

            suffix = '_%s_%s_%s_%s_%s' % (ORDER_ID, SUBORDER_ID,SUBPRODUCT_ID,EQUIPMENT_ID,EQUIPMENT_MODE)
            # 2.	Перед каждой операцией по обработке полуфабрикатов необходимо произвести переналадку;
            # 3.	Операции переналадки и обработки полуфабриката не могут происходить одновременно;
            duration = SETUP_TIME + MAKE_TIME

            start_var = self.model.NewIntVar(0,self.horizon,'start' + suffix)
            end_var = self.model.NewIntVar(0,self.horizon, 'end' + suffix)
            is_present_var = self.model.NewBoolVar('is_present' + suffix)
            interval_var = self.model.NewOptionalIntervalVar(   start_var, 
                                                                duration, 
                                                                end_var,
                                                                is_present_var,
                                                                'interval' + suffix)
            self.all_tasks[ORDER_ID, SUBORDER_ID,SUBPRODUCT_ID,EQUIPMENT_ID,EQUIPMENT_MODE] = task_type(start = start_var,
                                                                                                    end = end_var,
                                                                                                    present = is_present_var,
                                                                                                    interval= interval_var)
            #   Списки переменных для ограничений
            task_start[ORDER_ID,SUBORDER_ID,SUBPRODUCT_ID].append(start_var)
            task_end[ORDER_ID,SUBORDER_ID,SUBPRODUCT_ID].append(end_var)
            equip_present[ORDER_ID, SUBORDER_ID,SUBPRODUCT_ID].append(is_present_var)

            task_present[ORDER_ID,SUBORDER_ID].append(is_present_var)

            #   Время на перемещение
            task_start_move[ORDER_ID,SUBORDER_ID,SUBPRODUCT_ID,EQUIPMENT_ID].append(start_var)
            task_end_move[ORDER_ID,SUBORDER_ID,SUBPRODUCT_ID,EQUIPMENT_ID].append(end_var)
            task_present_move[ORDER_ID, SUBORDER_ID,SUBPRODUCT_ID,EQUIPMENT_ID].append(is_present_var)


            if EQUIPMENT_MODE == 'mode_0':    
                machine_to_intervals[EQUIPMENT_ID].append(interval_var) 



        #   Блок ограничений
        # **********************************************************************
        #   Старт и завершение операции равны 0 если интервальная переменная не используется  
        for item in self.interval_data:
            ORDER_ID = item[0]
            SUBORDER_ID = item[1] 
            SUBPRODUCT_ID = item[2] 
            EQUIPMENT_ID = item[3] 
            MAKE_TIME = item[4]
            UNIT = item[5] 
            EQUIPMENT_MODE = item[6] 
            SETUP_TIME = item[7] 
            self.model.Add(self.all_tasks[ORDER_ID, SUBORDER_ID,SUBPRODUCT_ID,EQUIPMENT_ID,EQUIPMENT_MODE].end == 0).OnlyEnforceIf(
                    self.all_tasks[ORDER_ID, SUBORDER_ID,SUBPRODUCT_ID,EQUIPMENT_ID,EQUIPMENT_MODE].present.Not()
                )
            self.model.Add(self.all_tasks[ORDER_ID, SUBORDER_ID,SUBPRODUCT_ID,EQUIPMENT_ID,EQUIPMENT_MODE].start == 0).OnlyEnforceIf(
                    self.all_tasks[ORDER_ID, SUBORDER_ID,SUBPRODUCT_ID,EQUIPMENT_ID,EQUIPMENT_MODE].present.Not()
                )            

        # 3.	Каждый конечный продукт в заказе имеет последовательность технологических операций, которую нельзя нарушать; 
        for item in self.imply_data:
            FROM_ORDER_ID = item[0] 
            FROM_SUBORDER_ID = item[1]
            FROM_SUBPRODUCT_ID = item[2]
            TO_ORDER_ID = item[3]
            TO_SUBORDER_ID = item[4]
            TO_SUBPRODUCT_ID = item[5]

            # 1.	Выбор оптимального оборудования для выполнения операции
            self.model.Add(sum (equip_present[FROM_ORDER_ID, FROM_SUBORDER_ID,FROM_SUBPRODUCT_ID]) ==
                sum (equip_present[TO_ORDER_ID, TO_SUBORDER_ID,TO_SUBPRODUCT_ID]))
            
            self.model.Add(sum (task_start[TO_ORDER_ID, TO_SUBORDER_ID,TO_SUBPRODUCT_ID]) >=
                sum (task_end[FROM_ORDER_ID, FROM_SUBORDER_ID,FROM_SUBPRODUCT_ID]))




        # 2.	Если режим работы оборудования соответствует mode_0, то одновременно на этом оборудовании 
        #       может выполнятся только одна операция;
        for machine in self.all_machines:
            self.model.AddNoOverlap(machine_to_intervals[machine])
        
        # Время перемещения полуфабрикатов продукции между цехами. 
        for item in self.start_stop_data:
            FROM_ORDER_ID = item[0]
            FROM_SUBORDER_ID = item[1]
            FROM_SUBPRODUCT_ID = item[2]
            FROM_EQUIPMENT_ID = item[3]
            TO_ORDER_ID = item[4]
            TO_SUBORDER_ID = item[5]
            TO_SUBPRODUCT_ID = item[6]
            TO_EQUIPMENT_ID = item[7]
            MOVE_TIME = item[8]
            decision = []
            decision.append(task_present_move[FROM_ORDER_ID, FROM_SUBORDER_ID,FROM_SUBPRODUCT_ID,FROM_EQUIPMENT_ID][0])
            decision.append(task_present_move[TO_ORDER_ID, TO_SUBORDER_ID,TO_SUBPRODUCT_ID,TO_EQUIPMENT_ID][0])

            self.model.Add(task_start_move[TO_ORDER_ID, TO_SUBORDER_ID,TO_SUBPRODUCT_ID,TO_EQUIPMENT_ID] >=
                task_end_move[FROM_ORDER_ID, FROM_SUBORDER_ID,FROM_SUBPRODUCT_ID,FROM_EQUIPMENT_ID] + [MOVE_TIME]).OnlyEnforceIf(
                    decision
                )


        
        #   Стратегия решения
        for item1 in self.all_orders:
            ORDER_ID_MAIN = item1          
            data = []
            for item in self.interval_data:
                ORDER_ID = item[0]  
                SUBORDER_ID = item[1] 
                SUBPRODUCT_ID = item[2]
                EQUIPMENT_ID = item[3] 
                MAKE_TIME = item[4]
                UNIT = item[5] 
                EQUIPMENT_MODE = item[6] 
                SETUP_TIME = item[7]
                if ORDER_ID_MAIN == ORDER_ID:
                    data.append(item)
            
            self.model.AddDecisionStrategy([self.all_tasks[ORDER_ID, SUBORDER_ID, SUBPRODUCT_ID, EQUIPMENT_ID,EQUIPMENT_MODE].present for item in data], 
                                                        cp_model.CHOOSE_HIGHEST_MAX,
                                                        cp_model.SELECT_MAX_VALUE
                                                        )

        obj_var = self.model.NewIntVar(0, self.horizon, 'makespan')
        self.model.AddMaxEquality(obj_var, [
                    self.all_tasks[item[0], item[1], item[2], item[3],item[6]].end      
                                                        for item in self.interval_data
        ])

        # 4.	Заказ может состоять из нескольких конечных продуктов. Частичное выполнение заказа к отчетной дате 
        #       добавляет 0 ед. к выручке;
        task_obj =  collections.defaultdict(list)
        obj_value =  collections.defaultdict(int)
        max_obj = 0
        for item in self.orders:
            #   Цены и финальные продукты
            # **********************************************************************
            ORDER_ID = item[0]
            QNT = item[1] 
            UNIT = item[2]
            PRICE = item[3]
            FIN_SUBORDER_ID = item[4]
            task_obj[ORDER_ID].append(task_present[ORDER_ID, FIN_SUBORDER_ID][0])
            obj_value[ORDER_ID] += PRICE
            max_obj += PRICE

        task_complete =  collections.defaultdict(list)
        task_exist =  collections.defaultdict(list)
        for item in self.all_orders:
            ORDER_ID = item
            suffix = '_%s' % (ORDER_ID)
            objective_var = self.model.NewIntVar(0,max_obj,'obj' + suffix)
            objective_var_present = self.model.NewBoolVar('obj' + suffix)
            task_complete[ORDER_ID].append(objective_var)
            task_exist[ORDER_ID].append(objective_var_present)

            self.model.Add(sum(task_obj[ORDER_ID]) == len(task_obj[ORDER_ID])*task_exist[ORDER_ID][0])
            self.model.Add(task_complete[ORDER_ID][0] == obj_value[ORDER_ID]*task_exist[ORDER_ID][0])



        #   Целевая функция
        # **********************************************************************
        if 'test' in self.file:
            self.model.Maximize(

                sum(task_complete[item][0] 
                                                        for item in self.all_orders)            
                - 
                obj_var # makespan
            )
        else:
            self.model.Maximize(

                sum(task_complete[item][0] 
                                                        for item in self.all_orders) 
            )            


    #   Запуск решения
    def solve(self):
        self.solver = cp_model.CpSolver()
        self.solver.parameters.max_time_in_seconds = 6000.0
        self.solver.parameters.log_search_progress = True
        self.solver.parameters.search_branching = cp_model.FIXED_SEARCH
        self.status = self.solver.Solve(self.model)
        print('Решение:')       
        print('Статус = %s' % self.solver.StatusName(self.status))
        print(f'Значение целевой функции: {self.solver.ObjectiveValue()}')   

    #   Выгрузка результатов 
    def output_data(self):
        if self.status == cp_model.OPTIMAL or self.status == cp_model.FEASIBLE:
            
            df = pd.DataFrame(columns=['OPERATION_TYPE', 'SUBORDER_ID', 'EQUIPMENT_ID', 'DATE_START','DATE_FIN'])
            for item in self.interval_data:
                ORDER_ID        =   item[0]
                SUBORDER_ID     =   item[1]
                SUBPRODUCT_ID   =   item[2]
                EQUIPMENT_ID    =   item[3] 
                MAKE_TIME       =   item[4]
                UNIT            =   item[5]
                EQUIPMENT_MODE  =   item[6]
                SETUP_TIME      =   item[7]
                start_interval = self.solver.Value(self.all_tasks[item[0], item[1], item[2], item[3],item[6]].start)
                end_interval = self.solver.Value(self.all_tasks[item[0], item[1], item[2], item[3],item[6]].end)
                present_interval = self.solver.Value(self.all_tasks[item[0], item[1], item[2], item[3],item[6]].present)
                
                if present_interval == 1:
                    #   Изготовление подзаказа
                    OPERATION_TYPE = 1
                    make_time_end = end_interval
                    make_time_start = end_interval - MAKE_TIME
                    sol_data = pd.DataFrame(
                                                [
                                                    (
                                                        OPERATION_TYPE, 
                                                        SUBORDER_ID,
                                                        EQUIPMENT_ID, 
                                                        make_time_start,
                                                        make_time_end
                                                    )
                                                ],
                                                    columns=['OPERATION_TYPE', 'SUBORDER_ID', 'EQUIPMENT_ID', 'DATE_START','DATE_FIN']
                                            )
                    df = df.append(sol_data,ignore_index=True)

                    if SETUP_TIME != 0:
                        #   Пераналадка оборудования
                        OPERATION_TYPE = 2
                        setup_time_start = start_interval
                        setup_time_end = start_interval + SETUP_TIME
                        sol_data = pd.DataFrame(
                                                [
                                                    (
                                                        OPERATION_TYPE, 
                                                        '',
                                                        EQUIPMENT_ID, 
                                                        setup_time_start,
                                                        setup_time_end
                                                    )
                                                ],
                                                    columns=['OPERATION_TYPE', 'SUBORDER_ID', 'EQUIPMENT_ID', 'DATE_START','DATE_FIN']
                                            )
                        df = df.append(sol_data,ignore_index=True)

            #   Простой оборудования
            for item in self.all_machines:
                EQUIPMENT_ID = item
                df_stop = pd.DataFrame(columns=['OPERATION_TYPE', 'SUBORDER_ID', 'EQUIPMENT_ID', 'DATE_START','DATE_FIN'])
                df_stop = df[df['EQUIPMENT_ID'] == EQUIPMENT_ID]
                df_stop.sort_values(by='DATE_START', inplace=True)
                #   Кол-во операций на оборудовании
                num_of_rows = df_stop.shape[0]
                #   Начало планирования
                start_horizon = 0
                #   Завершение планирования
                stop_horizon = self.horizon
                #   Счетчик
                i = 1
                for item in df_stop.itertuples(index=False):
                    OPERATION_TYPE = item[0] 
                    SUBORDER_ID = item[1] 
                    EQUIPMENT_ID = item[2] 
                    DATE_START = item[3] 
                    DATE_FIN = item[4]

                    #   Начало операций
                    if i == 1 and DATE_START > start_horizon:
                        #   Время со старта планирования
                        OPERATION_TYPE = 3
                        stop_time_start = start_horizon
                        stop_time_end = DATE_START
                        sol_data = pd.DataFrame(
                                                [
                                                    (
                                                        OPERATION_TYPE, 
                                                        '',
                                                        EQUIPMENT_ID, 
                                                        stop_time_start,
                                                        stop_time_end
                                                    )
                                                ],
                                                    columns=['OPERATION_TYPE', 'SUBORDER_ID', 'EQUIPMENT_ID', 'DATE_START','DATE_FIN']
                                            )
                        df = df.append(sol_data,ignore_index=True)
                    
                    #   Промежуточные операции
                    if i > 1 and i <= num_of_rows and TIME_END_PREV != DATE_START:
                        #   Время простоя между операциями
                        OPERATION_TYPE = 3
                        stop_time_start = TIME_END_PREV
                        stop_time_end = DATE_START
                        sol_data = pd.DataFrame(
                                                [
                                                    (
                                                        OPERATION_TYPE, 
                                                        '',
                                                        EQUIPMENT_ID, 
                                                        stop_time_start,
                                                        stop_time_end
                                                    )
                                                ],
                                                    columns=['OPERATION_TYPE', 'SUBORDER_ID', 'EQUIPMENT_ID', 'DATE_START','DATE_FIN']
                                            )
                        df = df.append(sol_data,ignore_index=True)

                    #   Финальный простой
                    if i == num_of_rows and DATE_FIN < stop_horizon:
                        #   Время простоя до конца горизонта планирования
                        OPERATION_TYPE = 3
                        stop_time_start = DATE_FIN
                        stop_time_end = stop_horizon
                        sol_data = pd.DataFrame(
                                                [
                                                    (
                                                        OPERATION_TYPE, 
                                                        '',
                                                        EQUIPMENT_ID, 
                                                        stop_time_start,
                                                        stop_time_end
                                                    )
                                                ],
                                                    columns=['OPERATION_TYPE', 'SUBORDER_ID', 'EQUIPMENT_ID', 'DATE_START','DATE_FIN']
                                            )
                        df = df.append(sol_data,ignore_index=True)                        


                    #   Время завершения предыдущей операции
                    TIME_END_PREV = DATE_FIN
                    i += 1

        import datetime            
        now = datetime.datetime.now()
        now = datetime.datetime.fromisoformat(now.strftime('%Y-%m-%d %H:%M'))

        df['DATE_START'] = now + pd.to_timedelta(df['DATE_START'], unit='m')
        df['DATE_FIN'] = now + pd.to_timedelta(df['DATE_FIN'], unit='m')        
        return   df     
