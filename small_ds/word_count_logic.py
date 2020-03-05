import string
import sys
import random




def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def split_data_by_word(data):
    # print('process started', file_name, chunk_number, len(data))
    my_word_list = ['muslim', 'islam']
    words_result = []
    the_year_wise = {}
    word_year_wise = {}
    year_index_in_ngram = 5
    n5grm_length = 8

    for line in data:
        sys.stdout.write("\r" + random.choice(string.ascii_letters))
        sys.stdout.flush()
        line = line.lower()

        for wd in my_word_list:
            if wd in line:
                # specified word moved to seperate dataset
                words_result.append(line)
                break

        # word 'the' counts year wise
        if "the" in line:
            t_w = line.split()
            if len(t_w) != n5grm_length:
                break
            year = 0
            if is_int(t_w[year_index_in_ngram]):
                year = t_w[year_index_in_ngram]
            if year == 0:
                break
            local_the = 0
            for w in t_w[0:5]:
                if "the" in w:
                    local_the = local_the + 1

            if local_the > 0:
                if is_int(t_w[6]):
                    no_of_occ = int(t_w[6])
                    local_the = local_the * no_of_occ
                    if year in the_year_wise:
                        the_year_wise[year] = the_year_wise[year] + local_the
                    else:
                        the_year_wise[year] = local_the

        non_of_the_word_found = True
        for word in my_word_list:
            if word in line:
                t_w = line.split()
                if len(t_w) != n5grm_length:
                    break
                year = 0
                if is_int(t_w[year_index_in_ngram]):
                    year = t_w[year_index_in_ngram]
                if year == 0:
                    break
                local_word = 0
                for w in t_w[0:5]:
                    if word in w:
                        local_word = local_word + 1
                # if (local_word > 0 &  one_of_the_word_found == False) or (one_of_the_word_found & local_word>1):
                if local_word > 0:
                    if is_int(t_w[6]):
                        no_of_occ = int(t_w[6])
                        if non_of_the_word_found == False & local_word > 1:
                            continue

                        local_word =  local_word * no_of_occ
                        non_of_the_word_found = False
                        if year in word_year_wise:
                            word_year_wise[year] = word_year_wise[year] + local_word
                        else:
                            word_year_wise[year] = local_word

    return word_year_wise,the_year_wise, words_result


content = ['islam good islam great muslim 1995 2 6',
           'islam muslim islam great muslim 1995 2 6',
           'islam muslim islam muslim muslim 1996 2 6']
output = split_data_by_word(content)
print (output)