import json 


def main():
    with open('tables_small.json', 'r') as j:
        lines = j.readlines()
        for line in lines: 
            contents = json.loads(line)
            import ipdb; ipdb.set_trace()



if __name__ == '__main__': 
    main()