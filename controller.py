__author__ = 'TramAnh'

from LP_implement import feature_graph_engineer, g_train_file, g_test_file
from serialize_classDist import write_prob_dist_json
# from mr_runner import *

json_train = 'json_train.txt'
json_test = 'json_test.txt'

infile = './Data/cleaned_data_encoded.csv'

feature_data_train, feature_data_test = feature_graph_engineer(infile)

write_prob_dist_json(g_train_file, json_train)
write_prob_dist_json(g_test_file, json_test)

# df_train = runner_train()   # Run MR for train graph
# df_test = runner_test()
#
# train_data = feature_data_train.join(df_train)
# test_data = feature_data_test.join(df_test)
#
# train_data.to_csv('train_data.csv', index=False)
# test_data.to_csv('test_data.csv', index=False)







