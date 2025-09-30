"""For turning our shelf object into a text file"""
import shelve

def create_index():
    """creates the index.txt file"""
    
    with shelve.open("OMEGA_NEW_INDEX") as omega:
        for term in sorted(list(omega.keys())):
            with open("index.txt", "a") as f:
                f.write(f"{term} | ")
                for posting in omega[term]:
                    f.write(f" {posting.document_id}|{posting.term_frequency}|{str(posting.positions)} | ")
                f.write("\n")




def create_index_of_index():
    """to be used by a file seek operation"""
    byte_ctr = 0
    cur_token = list()
    with open("index.txt", "r") as file:
        byte = file.read(1)
        while byte:
            if byte == " ":
                with open("index_of_index.txt", "a") as newdex:
                    newdex.write(f"{('').join(cur_token)} | {byte_ctr - len(cur_token)}\n")
                cur_token.clear()
                byte = file.readline()
                byte_ctr += len(byte)
            else:
                cur_token.append(byte)
            byte_ctr += 1
            byte = file.read(1)


            

if __name__ == "__main__":
    create_index()
    create_index_of_index()