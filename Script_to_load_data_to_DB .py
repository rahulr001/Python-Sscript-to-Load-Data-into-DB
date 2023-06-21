import psycopg2
import pandas as pd
from datetime import datetime

conn = psycopg2.connect(
    host="100.24.133.83",
    database="bis-erp",
    user="user1",
    password="changeme"
)
df1 = pd.read_excel("Electrical Item List.xlsx", sheet_name=0)
df2 = pd.read_excel("Plumbing Item List  Updated.xlsx", sheet_name=0)

cur = conn.cursor()


class Data_Dumper:
    def __init__(self):
        self.Company_ID = 'BUS_CMPNY'
        self.Company_Client_ID = 1005
        self.Gst = 0
        self.CreatedBy = 'prem'
        self.CreatedDtTm = datetime.now()

    def add_item_type(self,  Item_type):
        cur.execute(
            f'''SELECT * FROM bis."ItemType" WHERE "ItemTyp_ID" = '{Item_type}' ''')
        db_Item = cur.fetchone()
        if db_Item:
            print(
                f"Warning: An ItemType with similar name '{Item_type}' already exists.")
        elif pd.isnull(Item_type):
            print("NO Item type")
        else:
            cur.execute('INSERT INTO bis."ItemType" ( "ItemTyp_ID","ItemTyp_Company_ID","ItemTyp_Client_ID","ItemTyp_Descr","ItemTyp_CreatedBy","ItemTyp_CreatedDtTm") VALUES ( %s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                        (Item_type, self.Company_ID, self.Company_Client_ID, Item_type, self.CreatedBy, self.CreatedDtTm))
            conn.commit()
            print("Data added Successfully")

    def add_manufacturer(self,  Manufacturer, Item_type):
        cur.execute(
            f'''SELECT "ItemTyp_Key" FROM bis."ItemType" WHERE "ItemTyp_ID"= '{Item_type}' ''')
        Mfr_ItemTyp_Key = cur.fetchone()
        cur.execute(
            f'''SELECT * FROM bis."Manufacturer" WHERE "Mfr_ID" = '{Manufacturer}' ''')
        db_Item = cur.fetchall()
        if db_Item:
            print(
                f"Warning: An Manufacturer with similar name '{Manufacturer}' already exists.")
        elif pd.isnull(Manufacturer):
            print("NO Manufacturer")
        else:
            cur.execute('INSERT INTO bis."Manufacturer" ( "Mfr_ID","Mfr_Name","Mfr_Company_ID","Mfr_Client_ID","Mfr_ItemTyp_Key","Mfr_CreatedBy","Mfr_CreatedDtTm") VALUES ( %s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                        (Manufacturer, Manufacturer, self.Company_ID, self.Company_Client_ID, Mfr_ItemTyp_Key,  self.CreatedBy, self.CreatedDtTm))
            conn.commit()
            print("Data added Successfully")

    def add_UOMTyp(self, UomPar):
        try:
            uom = [i for i in UomPar.split(",")]
        except:
            uom = []
        while uom:
            for Uom in uom:
                cur.execute(
                    f'''SELECT * FROM bis."UOMType" WHERE "UOMTyp_ID" = '{Uom}' ''')
                db_Item = cur.fetchone()
                if db_Item:
                    print(
                        f"Warning: An UOMType with similar name '{Uom}' already exists.")
                elif pd.isnull(Uom):
                    print("NO UOM Type")
                else:
                    cur.execute('INSERT INTO bis."UOMType" ( "UOMTyp_ID","UOMTyp_Descr","UOMTyp_Company_ID","UOMTyp_Client_ID","UOMTyp_CreatedBy","UOMTyp_CreatedDtTm" ) VALUES ( %s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                                (Uom, Uom, self.Company_ID, self.Company_Client_ID,   self.CreatedBy, self.CreatedDtTm))
                    conn.commit()
                    print("Data added Successfully")
                uom.remove(Uom)

    def add_ItemUOM(self,  UomPar, Item_type):
        try:
            uom = [i for i in UomPar.split(",")]
        except:
            uom = []
        while uom:
            for Uom in uom:
                cur.execute(
                    f'''SELECT "ItemTyp_Key" FROM bis."ItemType" WHERE "ItemTyp_ID"= '{Item_type}' ''')
                ItemUOM_ItemTyp_Key = cur.fetchone()
                cur.execute(
                    f'''SELECT "UOMTyp_Key" FROM bis."UOMType" WHERE "UOMTyp_ID"= '{Uom}' ''')
                ItemUOM_UOMTyp_Key = cur.fetchone()
                cur.execute(
                    f'''SELECT * FROM bis."ItemUOM" WHERE "ItemUOM_ID" = '{Uom}' ''')
                db_Item = cur.fetchone()
                if db_Item:
                    print(
                        f"Warning: An ItemUOM with similar name '{Uom}' already exists.")
                elif pd.isnull(Uom):
                    print("NO UOM")
                else:
                    cur.execute('INSERT INTO bis."ItemUOM" ( "ItemUOM_ID","ItemUOM_Descr","ItemUOM_Company_ID","ItemUOM_Client_ID","ItemUOM_UOMTyp_Key","ItemUOM_ItemTyp_Key","ItemUOM_CreatedBy","ItemUOM_CreatedDtTm") VALUES ( %s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                                (Uom, Uom, self.Company_ID, self.Company_Client_ID, ItemUOM_UOMTyp_Key, ItemUOM_ItemTyp_Key,   self.CreatedBy, self.CreatedDtTm))
                    conn.commit()
                    print("Data added Successfully")
                uom.remove(Uom)

    def add_ItemSubType(self,  Item_sub_type, Item_type, UomPar):
        Uom_Keys = []
        ItemSubTyp_Uom_Keys = ''
        uom = [i for i in UomPar.split(',')]
        for Uom in uom:
            try:
                cur.execute(
                f'''SELECT "UOMTyp_Key" FROM bis."UOMType" WHERE "UOMTyp_ID"= '{Uom}' ''')
                Key = cur.fetchone()
                Uom_Keys.append(str(Key[0]))
                uom.remove(Uom)
            except:
                print('Uom not found')
        ItemSubTyp_Uom_Keys += '|'.join(Uom_Keys)
        cur.execute(
            f'''SELECT "ItemTyp_Key" FROM bis."ItemType" WHERE "ItemTyp_ID"= '{Item_type}' ''')
        ItemSubTyp_ItemTyp_Key = cur.fetchone()
        cur.execute(
            f'''SELECT * FROM bis."ItemSubType" WHERE "ItemSubTyp_ID" = '{Item_sub_type}' ''')
        db_Item = cur.fetchone()
        if db_Item:
            print(
                f"Warning: An ItemSubType with similar name '{Item_sub_type}' already exists.")
        elif pd.isnull(Item_sub_type):
            print("NO Item sub type")
        else:
            cur.execute('INSERT INTO bis."ItemSubType" ( "ItemSubTyp_ID","ItemSubTyp_Descr","ItemSubTyp_Company_ID","ItemSubTyp_Client_ID","ItemSubTyp_Uom_Keys","ItemSubTyp_ItemTyp_Key","ItemSubTyp_Gst","ItemSubTyp_CreatedBy","ItemSubTyp_CreatedDtTm") VALUES (  %s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                        (Item_sub_type, Item_sub_type, self.Company_ID, self.Company_Client_ID, ItemSubTyp_Uom_Keys, ItemSubTyp_ItemTyp_Key, self.Gst,   self.CreatedBy, self.CreatedDtTm))
            conn.commit()
            print("Data added Successfully")
        

    def add_item(self, Item_name, Item_sub_type, Manufacturer, Item_type, UomPar):
        try:
            uom = [i for i in UomPar.split(",")]
        except:
            uom = []
        Item_StockType = 'I'
        if uom is not None:
            while uom:
                for Uom in uom:
                    cur.execute(
                        f'''SELECT "ItemTyp_Key" FROM bis."ItemType" WHERE "ItemTyp_ID"= '{Item_type}' ''')
                    ItemTyp_key = cur.fetchone()
                    cur.execute(
                        f'''SELECT "ItemUOM_Key" FROM bis."ItemUOM" WHERE "ItemUOM_ID"= '{Uom}' ''')
                    ItemUOM_UOM_Key = cur.fetchone()
                    cur.execute(
                        f'''SELECT "UOMTyp_Key" FROM bis."UOMType" WHERE "UOMTyp_ID"= '{Uom}' ''')
                    ItemUOM_UOMTyp_Key = cur.fetchone()
                    cur.execute(
                        f'''SELECT "Mfr_Key" FROM bis."Manufacturer" WHERE "Mfr_ID" = '{Manufacturer}' ''')
                    Item_Manufacturer_key = cur.fetchone()
                    cur.execute(
                        f'''SELECT "ItemSubTyp_Key" FROM bis."ItemSubType" WHERE "ItemSubTyp_ID" = '{Item_sub_type}' ''')
                    ItemSubTyp_Key = cur.fetchone()
                    cur.execute(
                        f'''SELECT * FROM bis."Item" WHERE "Item_ID" = '{Item_name}' ''')
                    db_Item = cur.fetchone()
                    if db_Item:
                        print(
                            f"Warning: An item with similar name {Item_name} already exists.")
                    elif pd.isnull(Item_name):
                        print("NO Item name")
                    else:
                        cur.execute('INSERT INTO bis."Item" ( "Item_ID","Item_Descr","Item_StockType","Item_ItemTyp_Key","Item_ItemUOM_Key","Item_Mfr_Key","Item_Company_ID","Item_Client_ID","Item_SubType_Key","Item_ItemUOMs","Item_Gst","Item_CreatedBy","Item_CreatedDtTm") VALUES ( %s, %s, %s, %s, %s,%s, %s, %s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                                    (Item_name, Item_name, Item_StockType,  ItemTyp_key,  ItemUOM_UOM_Key, Item_Manufacturer_key, self.Company_ID, self.Company_Client_ID, ItemSubTyp_Key, ItemUOM_UOMTyp_Key, self.Gst,   self.CreatedBy, self.CreatedDtTm))
                        conn.commit()
                        print("Data added Successfully")
                    uom.remove(Uom)

    def add_ItemSpecification(self, Index, Specification_list, Item_name):
        try:
            values = [pair for pair in Specification_list.split(",")]
        except:
            values = []
        ItemSpec_ID = f"ItemSpec-{Index}"
        while values:
            for i in values:
                try:
                    Spec_name, Spec_value = i.split("=")
                    values.remove(i)
                except:
                    Spec_name, Spec_value = None, None
                    print("Data formate mismatch")
                    values.remove(i)
                cur.execute(
                    f'''SELECT "Item_Key" FROM bis."Item" WHERE "Item_ID" = '{Item_name}' ''')
                Item_Key = cur.fetchone()
                # cur.execute(
                #     f'''SELECT * FROM bis."ItemSpecification" WHERE "ItemSpec_ID" = '{ItemSpec_ID}' ''')
                # db_Item = cur.fetchone()
                # if db_Item:
                #     print(
                #         f"Warning: An ItemSpecification with similar name '{ItemSpec_ID}' already exists.")
                if pd.isnull(ItemSpec_ID):
                    print("NO Item Specification")
                else:
                    cur.execute('INSERT INTO bis."ItemSpecification" ( "ItemSpec_ID","ItemSpec_Name","ItemSpec_Value","ItemSpec_Client_ID","ItemSpec_Company_ID","ItemSpec_Item_key","ItemSpec_CreatedBy","ItemSpec_CreatedDtTm") VALUES (  %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING',
                                (ItemSpec_ID, Spec_name, Spec_value,  self.Company_Client_ID, self.Company_ID, Item_Key,   self.CreatedBy, self.CreatedDtTm))
                    conn.commit()
                    print("Data added Successfully")


def dataframe1():
    obj = Data_Dumper()
    for Index, row in df1.iterrows():
        obj.add_item_type(Item_type=row['Item Type'])

    for Index, row in df1.iterrows():
        obj.add_manufacturer(
            Manufacturer=row['Manufacturer'], Item_type=row['Item Type'])

    for Index, row in df1.iterrows():
        obj.add_UOMTyp(UomPar=row['UOM'])

    for Index, row in df1.iterrows():
        obj.add_ItemUOM(
            UomPar=row['UOM'], Item_type=row['Item Type'])

    for Index, row in df1.iterrows():
        obj.add_ItemSubType(
            Item_sub_type=row['Item SubType'], Item_type=row['Item Type'], UomPar=row['UOM'])

    for Index, row in df1.iterrows():
        obj.add_item(Item_name=row['Item Name'], Item_sub_type=row['Item SubType'],
                     Manufacturer=row['Manufacturer'], Item_type=row['Item Type'], UomPar=row['UOM'])

    for Index, row in df1.iterrows():
        index = "10"+str(Index+1)
        obj.add_ItemSpecification(
            Index=int(index), Specification_list=row['SpecificationListwithValue'], Item_name=row['Item Name'])


def dataframe2():
    obj = Data_Dumper()
    for Index, row in df2.iterrows():
        obj.add_item_type(Item_type=row['Item Type'])

    for Index, row in df2.iterrows():
        obj.add_manufacturer(
            Manufacturer=row['Manufacturer'], Item_type=row['Item Type'])

    for Index, row in df2.iterrows():
        obj.add_UOMTyp(UomPar=row['UOM'])

    for Index, row in df2.iterrows():
        obj.add_ItemUOM(
            UomPar=row['UOM'], Item_type=row['Item Type'])

    for Index, row in df2.iterrows():
        obj.add_ItemSubType(
            Item_sub_type=row['Item SubType'], Item_type=row['Item Type'], UomPar=row['UOM'])

    for Index, row in df2.iterrows():
        obj.add_item(Item_name=row['Item Name'], Item_sub_type=row['Item SubType'],
                     Manufacturer=row['Manufacturer'], Item_type=row['Item Type'], UomPar=row['UOM'])

    for Index, row in df2.iterrows():
        index = "20"+str(Index+1)
        obj.add_ItemSpecification(
            Index=index, Specification_list=row['SpecificationListwithValue'], Item_name=row['Item Name'])


dataframe1()
dataframe2()
