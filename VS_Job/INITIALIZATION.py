import pandas as pd

class preprocessing_data:

    def __init__(self, subproduct,structure,equipment,switch_time,order_graph,movement_time):
        self.subproduct = subproduct
        self.structure = structure 
        self.equipment = equipment   
        self.switch_time = switch_time 
        self.order_graph = order_graph
        self.movement_time = movement_time

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return True    

    def pre_interval_data(self):
        step_1 = pd.merge(
        left=self.structure,
        right=self.subproduct,
        left_on=['SUBPRODUCT_ID'],
        right_on=['SUBPRODUCT_ID']).drop_duplicates()

        step_2 = pd.merge(
        left=step_1,
        right=self.equipment,
        left_on=['EQUIPMENT_ID'],
        right_on=['EQUIPMENT_ID']).drop_duplicates()   

        step_3 = pd.merge(
        left=step_2,
        right=self.switch_time,
        how='left',
        left_on=['EQUIPMENT_ID','SUBPRODUCT_ID'],
        right_on=['EQUIPMENT_ID','SUBPRODUCT_ID']).drop_duplicates()  

        step_3.fillna(0, inplace=True)
        step_3.SETUP_TIME = step_3.SETUP_TIME.astype('int64')
        step_3.MAKE_TIME = step_3.MAKE_TIME.astype('int64')
        return step_3


    def imply_data(self):
        from_intervals = pd.merge(
        left=self.structure,
        right=self.order_graph,
        left_on=['SUBORDER_ID'],
        right_on=['TO_SUBORDER_ID']).drop_duplicates()


        to_intervals = pd.merge(
        left=self.structure,
        right=from_intervals,
        left_on=['SUBORDER_ID'],
        right_on=['FROM_SUBORDER_ID']).drop_duplicates()


        to_intervals.columns = ['FROM_ORDER_ID','FROM_SUBORDER_ID','FROM_SUBPRODUCT_ID',
                                'TO_ORDER_ID','TO_SUBORDER_ID','TO_SUBPRODUCT_ID','X','Y']

        col_data = to_intervals[['FROM_ORDER_ID','FROM_SUBORDER_ID','FROM_SUBPRODUCT_ID',
                                'TO_ORDER_ID','TO_SUBORDER_ID','TO_SUBPRODUCT_ID']].drop_duplicates()
        return col_data

    def start_stop_data(self):
        step1 = pd.merge(
                left=self.structure,
                right=self.subproduct,
                left_on=['SUBPRODUCT_ID'],
                right_on=['SUBPRODUCT_ID']).drop_duplicates()

        init_data = step1[['ORDER_ID','SUBORDER_ID','SUBPRODUCT_ID',
                                'EQUIPMENT_ID']].drop_duplicates()

        

        step2 = pd.merge(
                left=init_data,
                right=self.order_graph,
                left_on=['SUBORDER_ID'],
                right_on=['TO_SUBORDER_ID']).drop_duplicates()
        
        

        step3 = pd.merge(
                left=init_data,
                right=step2,
                left_on=['SUBORDER_ID'],
                right_on=['FROM_SUBORDER_ID']).drop_duplicates()

        step3.columns = ['FROM_ORDER_ID','FROM_SUBORDER_ID','FROM_SUBPRODUCT_ID','FROM_EQUIPMENT_ID',
                                'TO_ORDER_ID','TO_SUBORDER_ID','TO_SUBPRODUCT_ID','TO_EQUIPMENT_ID','X','Y']
        
        col_data = step3[['FROM_ORDER_ID','FROM_SUBORDER_ID','FROM_SUBPRODUCT_ID','FROM_EQUIPMENT_ID',
                                'TO_ORDER_ID','TO_SUBORDER_ID','TO_SUBPRODUCT_ID','TO_EQUIPMENT_ID']].drop_duplicates()

        pre_fin_data = pd.merge(
                left=col_data,
                right=self.movement_time,
                left_on=['FROM_EQUIPMENT_ID','TO_EQUIPMENT_ID','FROM_SUBPRODUCT_ID'],
                right_on=['FROM_EQUIPMENT_ID','TO_EQUIPMENT_ID','SUBPRODUCT_ID']).drop_duplicates()

        fin_data = pre_fin_data[['FROM_ORDER_ID','FROM_SUBORDER_ID','FROM_SUBPRODUCT_ID','FROM_EQUIPMENT_ID',
                                'TO_ORDER_ID','TO_SUBORDER_ID','TO_SUBPRODUCT_ID','TO_EQUIPMENT_ID','MOVE_TIME']].drop_duplicates()
        fin_data.MOVE_TIME = fin_data.MOVE_TIME.astype('int64')

        return fin_data