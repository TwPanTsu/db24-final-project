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

	private Constant low;
	private Constant high;
	private boolean lowIncl;
	private boolean highIncl;

	/**
	 * Constructs a new instance.
	 * 
	 * @param low
	 *            the lower bound. <code>null</code> means unbound.
	 * @param lowIncl
	 *            whether the lower bound is inclusive
	 * @param high
	 *            the higher bound. <code>null</code> means unbound.
	 * @param highIncl
	 *            whether the higher bound is inclusive
	 */

	VectorConstantRange(Constant low, boolean lowIncl,
			Constant high, boolean highIncl) {
		if(!low.equals(high))
			throw new IllegalArgumentException();
		this.low = low;
		this.lowIncl = lowIncl;

		this.high = high;
		this.highIncl = highIncl;
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
		return low;
	}

	@Override
	public Constant high() {
		return high;
	}

	@Override
	public boolean isLowInclusive() {
		return lowIncl;
	}

	@Override
	public boolean isHighInclusive() {
		return highIncl;
	}

	@Override
	public double length() {
		return 1;
	}

	/*
	 * Constant operations.
	 */

	@Override
	public ConstantRange applyLow(Constant c, boolean incl) {
		return null;
	}

	@Override
	public ConstantRange applyHigh(Constant c, boolean incl) {
		return null;
	}

	@Override
	public ConstantRange applyConstant(Constant c) {
		return null;
	}

	@Override
	public boolean isConstant() {
		return true;
	}

	@Override
	public Constant asConstant() {
		return low;
	}

	@Override
	public boolean contains(Constant c) {
		return c == low;
	}

	@Override
	public boolean lessThan(Constant c) {
		return c == low;
	}

	@Override
	public boolean largerThan(Constant c) {
        return c == low;
	}

	/*
	 * Range operations.
	 */

	@Override
	public boolean isOverlapping(ConstantRange r) {
		return false;
	}

	@Override
	public boolean contains(ConstantRange r) {
		return false;
	}

	@Override
	public ConstantRange intersect(ConstantRange r) {
		return null;
	}

	@Override
	public ConstantRange union(ConstantRange r) {
		return null;
	}
}
