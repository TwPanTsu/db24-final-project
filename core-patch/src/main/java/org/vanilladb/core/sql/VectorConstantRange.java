/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.sql;

public class VectorConstantRange extends ConstantRange {

	private VectorConstant v;

	/**
	 * Constructs a new single vector instance.
	 */
	public VectorConstantRange(VectorConstant v) {
		this.v = v;
	}

	/*
	 * Getters
	 */
	@Override
	public boolean isValid() {
		return true;
	}

	@Override
	public boolean hasLowerBound() {
		return true;
	}

	@Override
	public boolean hasUpperBound() {
		return true;
	}

	@Override
	public Constant low() {
		return v;
	}

	@Override
	public Constant high() {
		return v;
	}

	@Override
	public boolean isLowInclusive() {
		return true;
	}

	@Override
	public boolean isHighInclusive() {
		return true;
	}

	@Override
	public double length() {
		throw new UnsupportedOperationException();
	}

	/*
	 * Constant operations.
	 */

	@Override
	public ConstantRange applyLow(Constant c, boolean incl) {
		return new VectorConstantRange(v);
	}
	
	@Override
	public ConstantRange applyHigh(Constant c, boolean incl) {
		return new VectorConstantRange(v);
	}
	
	@Override
	public ConstantRange applyConstant(Constant c) {
		return new VectorConstantRange(v);
	}

	@Override
	public boolean isConstant() {
		return true;
	}

	@Override
	public Constant asConstant() {
		return v;
	}

	@Override
	public boolean contains(Constant c) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
		return v.compareTo(c) == 0;
	}

	@Override
	public boolean lessThan(Constant c) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
		return v.compareTo(c) < 0;
	}

	@Override
	public boolean largerThan(Constant c) {
		if (!(c instanceof VectorConstant))
			throw new IllegalArgumentException();
		return v.compareTo(c) > 0;
	}

	@Override
	public boolean isOverlapping(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
		return contains(r);
	}

	@Override
	public boolean contains(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
		return ((VectorConstantRange) r).v.compareTo(v) == 0;
	}

	@Override
	public ConstantRange intersect(ConstantRange r) {
		if (!(r instanceof VectorConstantRange))
			throw new IllegalArgumentException();
		if (contains(r))
			return new VectorConstantRange(v);
		else
			return null;
	}

	@Override
	public ConstantRange union(ConstantRange r) {
		return intersect(r);
	}
}
