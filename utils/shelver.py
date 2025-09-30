import shelve

def run() -> None:
    with shelve.open("util_shelve") as da_shelve:
        da_shelve['1'] = "le_poste"
        print('1' in da_shelve)
        print('deleting \'1\'')
        del da_shelve['1']
        print('1' in da_shelve)



if __name__ == '__main__':
    run()