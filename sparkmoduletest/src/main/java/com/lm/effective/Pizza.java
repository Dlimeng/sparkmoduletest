package com.lm.effective;

import java.util.EnumSet;
import java.util.Set;

/**
 * @Author: limeng
 * @Date: 2019/9/26 11:35
 */
public abstract class Pizza {
    public enum Topping {HAM, MUSHROOM, ONION, PEPPER, SAUSAGE}
  //  final Set<Topping> toppings;

    abstract static class Builder<T extends Builder<T>> {


    }


}
