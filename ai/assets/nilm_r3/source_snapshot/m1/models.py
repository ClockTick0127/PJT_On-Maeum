"""Fixed-window causal models. Inputs B,T,4; all_logits is for causal tests."""
import math,torch
from torch import nn
from torch.nn import functional as F
FAMILIES=['TCN','GRU','LSTM','Transformer']
class Block(nn.Module):
 def __init__(self,cin,d):
  super().__init__();self.left=2*d;self.conv=nn.Conv1d(cin,64,3,dilation=d);self.res=nn.Identity() if cin==64 else nn.Conv1d(cin,64,1)
 def forward(self,x):return F.relu(self.conv(F.pad(x,(self.left,0)))+self.res(x))
class CausalModel(nn.Module):
 def __init__(self,family):
  super().__init__();assert family in FAMILIES;self.family=family
  if family=='TCN':self.body=nn.Sequential(*[Block(4 if i==0 else 64,d) for i,d in enumerate([1,2,4,8,16,32,64])]);self.head=nn.Linear(64,1)
  elif family in ['GRU','LSTM']:
   self.body=getattr(nn,family)(4,128,num_layers=2,batch_first=True,dropout=0,bidirectional=False);self.head=nn.Linear(128,1)
  else:
   self.embed=nn.Linear(4,128);layer=nn.TransformerEncoderLayer(128,4,512,dropout=0,activation='relu',batch_first=True,norm_first=True)
   self.body=nn.TransformerEncoder(layer,3,enable_nested_tensor=False);self.head=nn.Linear(128,1)
   t=torch.arange(255).float()[:,None];scale=torch.exp(torch.arange(0,128,2)*(-math.log(10000)/128));pe=torch.zeros(255,128);pe[:,0::2]=torch.sin(t*scale);pe[:,1::2]=torch.cos(t*scale);self.register_buffer('position',pe)
 def all_logits(self,x):
  assert x.ndim==3 and x.shape[-1]==4 and 1<=x.shape[1]<=255
  if self.family=='TCN':z=self.body(x.transpose(1,2)).transpose(1,2)
  elif self.family in ['GRU','LSTM']:z,_=self.body(x) # None state: zeros afresh for each window.
  else:
   z=self.embed(x)+self.position[:x.shape[1]];mask=torch.triu(torch.ones(x.shape[1],x.shape[1],device=x.device,dtype=torch.bool),diagonal=1)
   z=self.body(z,mask=mask,is_causal=True)
  return self.head(z).squeeze(-1)
 def forward(self,x):return self.all_logits(x)[:,-1]
