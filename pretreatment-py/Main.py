from smart_open import smart_open

if __name__ == "__main__":
    for line in smart_open("F:/wiki/enwiki-latest-all-titles.gz"):
        print(line)
