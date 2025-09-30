import shelve

def filter_shelve():
    """
    Filters content from OMEGA_INDEX to OMEGA_NEW_INDEX
    """
    with shelve.open("OMEGA_INDEX") as omega:
        with shelve.open("OMEGA_NEW_INDEX") as new_omega:
            for token in omega.keys():
                if not evaluate_token(token):
                    new_omega[token] = omega[token]

def evaluate_token(token: str) -> bool:
    """
    Given a token, returns true if there are "too many" numbers
    """
    count = 0
    if len(token) <= 5: return False
    for char in token:
        if char.isdigit(): count += 1
        if count >= 5: return True
    return False

def print_shelve():
    """
    Writes OMEGA_INDEX contents to a text file
    """
    with shelve.open("OMEGA_NEW_INDEX") as omega:
        with open("OMEGA_INDEX_CONTENT.txt", "w") as content:
            for token in omega.keys():
                content.write(f"{token} | {omega[token]}\n")

if __name__ == '__main__':
    filter_shelve()
    print_shelve()