import json 
import pandas as pd 

def main():
    with open('tables_small.json', 'r') as j:
        lines = j.readlines()
        for line in lines: 
            contents = json.loads(line)
            table = {}
            col_order = []
            for i, col in enumerate(contents['tableHeaders'][0]): 
                col_name = col['text']
                table[col_name] = []
                col_order.append(col_name)

            for row_cell in range(len(contents)):
                for col_cell in range(len(contents[0])):
                    col_name = col_order[col_cell]
                    data = contents['tableData'][row_cell][col_cell]['text']
                    table[col_name].append(data)

            pd.DataFrame.from_dict(table)
            import ipdb; ipdb.set_trace()


if __name__ == '__main__': 
    main()