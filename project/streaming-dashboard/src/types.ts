export interface Transaction {
  step: number;
  type: string;
  amount: number;
  nameOrig: string;
  oldbalanceOrg: number;   
  newbalanceOrig: number;   
  nameDest: string;
  oldbalanceDest: number;
  newbalanceDest: number;
  isFraud: number;         
}

export interface DashboardStats {
  totalCount: number;  
  fraudCount: number;  
  totalAmount: number;  
}

