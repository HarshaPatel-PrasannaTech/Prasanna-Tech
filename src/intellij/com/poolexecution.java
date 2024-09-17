/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package intellij.com;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *
 * @author bhuvaneshwari
 */
public class poolexecution {
    public static void main(String[] args) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);

        for (int i = 1; i <= 100; i++) {
            task task = new task("Task ::" + i);
            System.out.println("Created : " + task.getName());

            executor.execute(task);
        }
        executor.shutdown();
    }
}
