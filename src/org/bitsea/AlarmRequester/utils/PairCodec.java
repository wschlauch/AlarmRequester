package org.bitsea.AlarmRequester.utils;

import java.nio.ByteBuffer;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class PairCodec extends TypeCodec<Pair>{

	private final TypeCodec<UDTValue> innerCodec;
	private final UserType userType;
	
	public PairCodec(TypeCodec<UDTValue> innerCodec, Class<Pair> javaType) {
		super(innerCodec.getCqlType(), javaType);
		this.innerCodec = innerCodec;
		this.userType = (UserType)innerCodec.getCqlType();
	}
	
	
	@Override
	public ByteBuffer serialize(Pair value, ProtocolVersion protocol) throws InvalidTypeException {
		return innerCodec.serialize(toUDTValue(value), protocol);
	}
	
	
	@Override
	public Pair deserialize(ByteBuffer bytes, ProtocolVersion protocol) {
		return toPair(innerCodec.deserialize(bytes, protocol));
	}
	
	
	@Override
	public Pair parse(String value) throws InvalidTypeException {
		return value == null || value.isEmpty() || value.equals(null) ? null : toPair(innerCodec.parse(value));
	}
	
	
	@Override
	public String format(Pair value) throws InvalidTypeException {
		return value == null ? null : innerCodec.format(toUDTValue(value));
	}
	

	protected Pair toPair(UDTValue value) {
		return value == null ? null : new Pair(value.getFloat(0), value.getFloat(1));
	}
	
	
	protected UDTValue toUDTValue(Pair value) {
		return value == null ? null : userType.newValue()
				.setFloat(0, value.left).setFloat(1, value.right);
	}
}
