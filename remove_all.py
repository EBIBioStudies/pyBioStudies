import shutil

import dal.biostudies.biostudies_transaction as bs_transaction
import dal.biostudies.submittion as bs_submission
import dal.biostudies.user as bs_user
from settings import *

# bs_transaction.db = BIOSTUDIES_BETA
# bs_submission.db = BIOSTUDIES_BETA
# bs_user.db = BIOSTUDIES_BETA


__author__ = 'Ahmed G. Ali'

BACKEND_PATH = '/nfs/biostudies/.adm/databases/beta/submission'


def main():
    users = []
    subs = bs_transaction.get_ae_submissions()
    for sub in subs:
        sub_id = sub['id']
        acc = sub['accNo']
        # user_id = sub['owner_id']
        # users.append(user_id)
        if not (acc.startswith('E-') or acc.startswith('A-')):
            continue

        # rel_path = sub['relPath']
        # if rel_path:
        #     sub_path = os.path.join(BACKEND_PATH, rel_path)
        #     if not os.path.exists(sub_path):
        #         sub_path = '/'.join(sub_path.split('/')[:-1])
        #
        #     try:
        #         print("Removing: " + sub_path)
        #         shutil.rmtree(sub_path)
        #     except Exception as e:
        #         print(e)
        #         print('=' * 50)

        # print("deleting submission "+acc)
        bs_submission.remove_submission(sub_id=sub_id)
    # for user in users:
    #     if user < 10:
    #         continue
    #     u_subs = bs_submission.get_subscount_by_userid(user)[0]
    #     # print(u_subs['count'])
    #     if u_subs['count'] == 0:
    #         bs_user.delete_user_by_id(user)


if __name__ == '__main__':
    main()
