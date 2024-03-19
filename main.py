from dwh import update_dwh
from fraud import find_fraud


def main(date):
    update_dwh('data', date)
    find_fraud(date)


if __name__ == '__main__':
    for i in range(1, 4):
        main(f'0{i}032021')
