"use client";

import { Button } from "@relume_io/relume-ui";
import React from "react";

export function Faq7() {
  return (
    <section id="relume" className="px-[5%] py-16 md:py-24 lg:py-28">
      <div className="container w-full max-w-lg">
        <div className="rb-12 mb-12 text-center md:mb-18 lg:mb-20">
          <h2 className="rb-5 mb-5 text-5xl font-bold md:mb-6 md:text-7xl lg:text-8xl">
            FAQs
          </h2>
          <p className="md:text-md">
            Find answers to your questions about the Fix4more process and how it
            works.
          </p>
        </div>
        <div className="grid grid-cols-1 gap-x-12 gap-y-10 md:gap-y-12">
          <div>
            <h2 className="mb-3 text-base font-bold md:mb-4 md:text-md">
              What is Fix4more?
            </h2>
            <p>
              Fix4more allows you to buy items with minor defects at a
              discounted price. You can repair these items and sell them for a
              profit. This model benefits both buyers and sellers by providing
              savings and potential earnings.
            </p>
          </div>
          <div>
            <h2 className="mb-3 text-base font-bold md:mb-4 md:text-md">
              How do I start?
            </h2>
            <p>
              To get started with Fix4more, browse our selection of discounted
              items. Choose the products you want to purchase and follow the
              checkout process. Once you receive your items, you can begin the
              repair process.
            </p>
          </div>
          <div>
            <h2 className="mb-3 text-base font-bold md:mb-4 md:text-md">
              What items are available?
            </h2>
            <p>
              We offer a variety of items with minor defects across different
              categories. These can include electronics, furniture, and home
              goods. Each listing details the specific imperfections and
              potential for repair.
            </p>
          </div>
          <div>
            <h2 className="mb-3 text-base font-bold md:mb-4 md:text-md">
              Is there a warranty?
            </h2>
            <p>
              Items sold through Fix4more are typically sold as-is, meaning
              there is no warranty. However, we provide detailed descriptions of
              each item’s condition. This transparency helps you make informed
              purchasing decisions.
            </p>
          </div>
          <div>
            <h2 className="mb-3 text-base font-bold md:mb-4 md:text-md">
              Can I resell items?
            </h2>
            <p>
              Yes, you can resell items after repairing them. Many customers
              successfully turn their repairs into profit by reselling on
              various platforms. Just ensure you comply with any local
              regulations regarding resale.
            </p>
          </div>
        </div>
        <div className="mx-auto mt-12 max-w-md text-center md:mt-18 lg:mt-20">
          <h4 className="mb-3 text-2xl font-bold md:mb-4 md:text-3xl md:leading-[1.3] lg:text-4xl">
            Still have questions?
          </h4>
          <p className="md:text-md">We're here to help you!</p>
          <div className="mt-6 md:mt-8">
            <Button title="Contact" variant="secondary">
              Contact
            </Button>
          </div>
        </div>
      </div>
    </section>
  );
}
