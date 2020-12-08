import json 
import pandas as pd 

def main():
    with open('tables_small.json', 'r') as j:
        lines = j.readlines()
        tbls = {}
        for line in lines: 
            contents = json.loads(line)
            table = {}
            col_order = []
            for i, col in enumerate(contents['tableHeaders'][0]): 
                col_name = col['text']
                table[col_name] = []
                col_order.append(col_name)
            for row_cell in range(len(contents['tableData'])):
                for col_cell in range(len(contents['tableData'][row_cell])):
                    col_name = col_order[col_cell]
                    data = contents['tableData'][row_cell][col_cell]['text']
                    if data == '': 
                        continue
                    table[col_name].append(data)    
            try: 
                tbl = pd.DataFrame.from_dict(table)
                caption = contents['tableCaption']
                title = contents['pgTitle']
                sec_title = contents['sectionTitle']
                table_info = {}
                table_info['data'] = tbl 
                table_info['sec_title'] = sec_title 
                table_info['title'] = title 
                tbls[caption] = table_info
            except Exception as e:
                print('SKIPPING') 
                continue 

        print(tbls)

    

if __name__ == '__main__': 
    main()