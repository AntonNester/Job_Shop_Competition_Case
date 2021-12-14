import pandas as pd

class load_from_file:
    
    def __init__(self, file):
        self.file = file    
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return True    
    
    def load_equipment(self):
        # equipment_id
        # Уникальный идентификатор единицы оборудования, на котором выполняется работа. 
        # Например, это идентификатор элемента конвейера на сборочной линии.
        # equipment_mode	
        # Режим, в котором может работать оборудования. 
        # •	mode_0 означает, что на этом оборудовании может выполнятся операция только над одним полуфабрикатом. 
        # Например, это окраска корпуса в красильной камере.
        # •	mode_1 означает, что на этом оборудовании может выполнятся операция для неограниченного количества полуфабрикатов. 
        # Например, в камере сушки может расположиться любое количество полуфабрикатов. 
        equipment = pd.read_excel(self.file, 
                                sheet_name='equipment',
                                usecols="B:C",
                                dtype= {'equipment_id':str, 'equipment_mode':str}) 
        equipment.columns = ['EQUIPMENT_ID', 'EQUIPMENT_MODE']
        return equipment

    def load_subproduct(self):
        # subproduct_id	Уникальный идентификатор полуфабриката, необходимого для производства подзаказа. 	string
        # equipment_id	Уникальный идентификатор оборудования на котором производится полуфабрикат.	string
        # duration, min	Длительность изготовления полуфабриката subproduct_id на оборудовании equipment_id. 	float
        # unit	Единица измерения 	string

        subproduct = pd.read_excel(self.file, 
                                sheet_name='subproduct',
                                usecols="B:E",
                                dtype= {'subproduct_id':str, 'equipment_id':str,
                                'duration':float,'unit':str}) 
        subproduct.columns = ['SUBPRODUCT_ID', 'EQUIPMENT_ID', 'MAKE_TIME', 'UNIT']
        subproduct.MAKE_TIME = subproduct.MAKE_TIME.round()
        subproduct.MAKE_TIME = subproduct.MAKE_TIME.astype('int64')
        return subproduct

    def load_switch_time(self):
        # equipment_id	Уникальный идентификатор оборудования, на котором может 
        # изготавливаться полуфабрикат subproduct_id. 	string
        # subproduct_id	Уникальный идентификатор полуфабриката. 	string
        # duration, min	Время переналадки перед началом изготовления 
        # для оборудования с уникальным идентификатором equipment_id, 
        # чтобы оно могло выполнять изготовление полуфабриката  subproduct_id.
        #  Например, на equipment_id выполняется покраска конструкции, для того, чтобы поменять цвет покраски, 
        # необходимо промыть камеры с краской и заправить камеры с краской материалом другого цвета. 	float

        switch_time = pd.read_excel(self.file, 
                                sheet_name='switch_time',
                                usecols="B:D",
                                dtype= {'equipment_id':str, 'subproduct_id':str,
                                'duration':float}) 
        switch_time.columns = ['EQUIPMENT_ID', 'SUBPRODUCT_ID', 'SETUP_TIME']
        switch_time.SETUP_TIME = switch_time.SETUP_TIME.round()
        switch_time.SETUP_TIME = switch_time.SETUP_TIME.astype('int64')
        return switch_time

    def load_structure(self):
        # order_id	Идентификатор итогового заказа.	string
        # suborder_id	Идентификатор составляющих заказа. 
        # То есть указывает из каких элементов выполняется сборка итогового заказа. 	string
        # subproduct_id	Идентификатор полуфабриката. 
        # Он указывает на то, на каком оборудовании должна проводиться операция, 
        # чтобы выполнить составляющую заказа. Количество составляющих заказа меньше, чем количество операций. 	string

        structure = pd.read_excel(self.file, 
                                sheet_name='structure',
                                usecols="B:D",
                                dtype= {'order_id':str, 'suborder_id':str,
                                'subproduct_id':str}) 
        structure.columns = ['ORDER_ID', 'SUBORDER_ID', 'SUBPRODUCT_ID']
        return structure


    def load_order_graph(self):
        # from_suborder_id	Идентификатор подзаказа. То есть подзаказ, который должен быть 
        # выполнен перед началом изготовления  to_suborder_id.	string
        # to_suborder_id	Идентификатор подзаказа, который может быть выполнен после 
        # выполнения подзаказа from_suborder_id. 	string

        order_graph = pd.read_excel(self.file, 
                                sheet_name='order_graph',
                                usecols="B:C",
                                dtype= {'from_suborder_id':str, 'to_suborder_id':str}) 
        order_graph.columns = ['FROM_SUBORDER_ID','TO_SUBORDER_ID']
        return order_graph

    def load_movement_time(self):
        # from_equipment_id	Уникальный идентификатор оборудования с которого перемещается полуфабрикат. 	string
        # to_equipment_id	Уникальный идентификатор оборудования на который перемещается полуфабрикат.	string
        # subproduct_id	Уникальный идентификатор полуфабриката.  	string
        # duration, min	Время перемещения полуфабриката subproduct_id от  
        # единицы оборудования с уникальным идентификатором from_equipment_id на единицу 
        # оборудования с уникальным идентификатором to_equipment_id.	float

        movement_time = pd.read_excel(self.file, 
                                sheet_name='movement_time',
                                usecols="B:E",
                                dtype= {'from_equipment_id':str, 'to_equipment_id':str,
                                'subproduct_id':str,'duration':float}) 
        movement_time.columns = ['FROM_EQUIPMENT_ID','TO_EQUIPMENT_ID','SUBPRODUCT_ID','MOVE_TIME']
        return movement_time

    def load_orders(self):
        # order_id	Уникальный идентификатор заказа	string
        # quantity	Необходимый объем заказа	float
        # unit	Единица измерения заказа	string
        # price	Стоимость заказа. Важно отметить, что стоимость дублируется для всех подзаказов в заказе. 
        # В виду этого необходимо брать только одно значение для одного заказа. 	float
        # final_suborder_id	Идентификатор полуфабриката, который характеризует конечную продукцию. 
        # Например, для производства грузовика необходимо отдельно произвести кабину, платформу и т.д. 
        # Таким образом конечный грузовик будет считаться заказом, в то же время его подчасти будут 
        # являться конечными продуктами.	string

        orders = pd.read_excel(self.file, 
                                sheet_name='orders',
                                usecols="B:F",
                                dtype= {'order_id':str, 'quantity':float,
                                'unit':str,'price':float,'final_suborder_id':str})                     
        orders.columns = ['ORDER_ID','QNT','UNIT','PRICE','FIN_SUBORDER_ID']
        orders.PRICE = orders.PRICE.astype('int64')    
        return orders