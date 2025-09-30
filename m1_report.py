#import indexer.indexer as indexer
import os.path
import shelve

FILE_NAME = "OMEGA_INDEX-1"

def generate_report() -> None:
    with open("m1_report.txt", "w") as report:
        if os.path.exists(FILE_NAME):
            report.write(f"Number of documents: {55392 // 3}\n") # info from structurer.py
            report.write(f"Shelve size: {os.path.getsize(FILE_NAME) / 1024} KB\n")            
            with shelve.open(FILE_NAME) as omega:
                report.write(f"Number of unique tokens: {len(omega)}\n")
                for token in omega.keys():
                    report.write(f"{token} | {(', ').join([(posting.document_id) for posting in omega[token]])}\n")
            

           
        else:
            raise FileNotFoundError("No OMEGA_INDEX!")
        
    

if __name__ == '__main__':
    generate_report()