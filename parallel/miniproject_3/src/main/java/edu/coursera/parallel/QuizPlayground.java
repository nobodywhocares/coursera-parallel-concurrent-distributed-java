/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.coursera.parallel;

import static edu.rice.pcdp.PCDP.*;

/**
 *
 * @author noahn
 */
public class QuizPlayground {

    public static void main(String[] args) {
        try {
            int N = 3;
            int[] x = {1, 3, 5};
            int[] y = {9, 11, 13};
            int[] w = {1, 3, 5};
            int[] z = {9, 11, 13};
            
            for (int i = 0; i <= N; i = i + 1) {
                x[i] = x[i] + y[i];
                y[i + 1] = w[i] + z[i];
            }
// Below is PCDP parallel equivalent of "for" loop immediately above
//            x[0] = x[0] + y[0];
//            forall(i : [0 : N - 1]) {
//                y[i + 1] = w[i] + z[i];
//                x[i + 1] = x[i + 1] + y[i + 1];
//            }
//            y[N + 1] = w[N] + z[N];
            System.exit(0);
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
