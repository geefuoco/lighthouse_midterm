import pickle
import os


DIR_PATH = os.path.abspath(os.path.join(__file__, "../../../../data"))

def save_model(model, name):
  file = os.path.join(DIR_PATH, name)
  bool = False
  try:
    pickle.dump(model, open(file, "wb"))
    bool = True
  except:
    print("Error while saving model to file")
  return bool  
  
    
def load_model(name):
  try:
    file = os.path.join(DIR_PATH, name)
    model = pickle.load(open(file, "rb"))
    return model
  except:
    print("Error while trying to load model file")