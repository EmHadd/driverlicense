
def compute_sum(a, b):
    return a+b


def compute_multiply(a,b):
    return a*b


def compute_substract(a,b):
    return a-b


print('1+3=' + str(compute_sum(1,3)))


a = [1,2,3,4,5]
b = [2,3,2,3,2]

for i in range(0,5):
    formatlist1 = [a[i], b[i], compute_sum(a[i], b[i])]
    formatlist2 = [a[i], b[i], compute_multiply(a[i], b[i])]
    print('The sum of {} and {} is {}'.format(*formatlist1))
    print('The multiplication of {} and {} is {}'.format(*formatlist2))


print("name is ",__name__)