package io.confluent.developer;

import org.apache.kafka.streams.kstream.Reducer;

public class TxnReducer<Transactions> implements Reducer<Transactions> {

	@Override
	public Transactions apply(Transactions value1, Transactions value2) {
		// TODO Auto-generated method stub
		
		return null;
	}

	
//	
////    new Reducer<Long>() { /* adder */
////        @Override
////        public Long apply(Long aggValue, Long newValue) {
////          return aggValue + newValue;
////        }
//
//	public Transactions apply(Transactions value1, Transactions value2) {
//		// TODO Auto-generated method stub
//		value1.setAmount(value1.getAmount() + value2.getAmount());
//		return value1;
//	}
      
}
