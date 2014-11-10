/******************************************************************************
 *                                                                             *
 *  Copyright: (c) Syncleus, Inc.                                              *
 *                                                                             *
 *  You may redistribute and modify this source code under the terms and       *
 *  conditions of the Open Source Community License - Type C version 1.0       *
 *  or any later version as published by Syncleus, Inc. at www.syncleus.com.   *
 *  There should be a copy of the license included with this file. If a copy   *
 *  of the license is not included you are granted no right to distribute or   *
 *  otherwise use this file except through a legal and valid license. You      *
 *  should also contact Syncleus, Inc. at the information below if you cannot  *
 *  find a license:                                                            *
 *                                                                             *
 *  Syncleus, Inc.                                                             *
 *  2604 South 12th Street                                                     *
 *  Philadelphia, PA 19148                                                     *
 *                                                                             *
 ******************************************************************************/
package com.syncleus.grail.neural.activation;

import junit.framework.Assert;
import org.junit.Test;

public class IdentityActivationFunctionTest {
    private final static ActivationFunction ACTIVATION_FUNCTION = new IdentityActivationFunction();
    private final static double[][] ACTIVATION_TRUTH_TABLE = new double[][]{ {0.0, 0.0},
                                                                             {0.25, 0.25},
                                                                             {1.0, 1.0},
                                                                             {10.0, 10.0},
                                                                             {1000000.0, 1000000.0},
                                                                             {-0.25, -0.25},
                                                                             {-1.0, -1.0},
                                                                             {-10.0, -10.0},
                                                                             {-1000000.0, -1000000.0} };
    private final static double[][] DERIVATIVE_TRUTH_TABLE = new double[][]{ {0.0, 1.0},
                                                                             {0.25, 1.0},
                                                                             {1.0, 1.0},
                                                                             {10.0, 1.0},
                                                                             {1000000.0, 1.0},
                                                                             {-0.25, 1.0},
                                                                             {-1.0, 1.0},
                                                                             {-10.0, 1.0},
                                                                             {-1000000.0, 1.0} };
    private final static boolean IS_BOUND = false;
    private final static double UPPER_LIMIT = Double.MAX_VALUE;
    private final static double LOWER_LIMIT = -1.0 * Double.MAX_VALUE;

    @Test
    public void testActivation() {
        for( int index = 0; index < ACTIVATION_TRUTH_TABLE.length ; index++ )
            Assert.assertTrue(IdentityActivationFunctionTest.checkResult(ACTIVATION_FUNCTION.activate(ACTIVATION_TRUTH_TABLE[index][0]), ACTIVATION_TRUTH_TABLE[index][1]));
    }

    @Test
    public void testDerivative() {
        for( int index = 0; index < DERIVATIVE_TRUTH_TABLE.length ; index++ )
            Assert.assertTrue(IdentityActivationFunctionTest.checkResult(ACTIVATION_FUNCTION.activateDerivative(DERIVATIVE_TRUTH_TABLE[index][0]), DERIVATIVE_TRUTH_TABLE[index][1]));
    }

    @Test
    public void testIsBound() {
        Assert.assertTrue(ACTIVATION_FUNCTION.isBound() == IS_BOUND);
    }

    @Test
    public void testUpperLimit() {
        final double upperLimit = ACTIVATION_FUNCTION.getUpperLimit();
        Assert.assertTrue( upperLimit > 1.0 );
        Assert.assertTrue( IdentityActivationFunctionTest.checkResult(upperLimit, UPPER_LIMIT));
    }

    @Test
    public void testLowerLimit() {
        final double lowerLimit = ACTIVATION_FUNCTION.getLowerLimit();
        Assert.assertTrue( lowerLimit < -1.0 );
        Assert.assertTrue( IdentityActivationFunctionTest.checkResult(lowerLimit, LOWER_LIMIT));
    }

    private static boolean checkResult(final double firstValue, final double secondValue) {
        return (Math.abs(firstValue - secondValue) < 0.0000001);
    }
}
