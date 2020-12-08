import json 


def main():
    with open('tables_small.json', 'r') as j:
        contents = json.loads(j.read())
        import ipdb; ipdb.set_trace()



if __name__ == '__main__': 
    main()