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
            Here are some frequently asked questions to help you find the
            information you need.
          </p>
        </div>
        <div className="grid grid-cols-1 gap-x-12 gap-y-10 md:gap-y-12">
          <div>
            <h2 className="mb-3 text-base font-bold md:mb-4 md:text-md">
              How does Bid4more work?
            </h2>
            <p>
              Bid4more is an auction platform where you can bid on items at
              significantly reduced prices. You can participate in live auctions
              and compete with other bidders. The thrill of winning a deal makes
              it an exciting shopping experience.
            </p>
          </div>
          <div>
            <h2 className="mb-3 text-base font-bold md:mb-4 md:text-md">
              What is Bin4more?
            </h2>
            <p>
              Bin4more offers a unique pricing cycle for essential items.
              Starting every Friday, items are listed at $10 and decrease in
              price throughout the week. This encourages early purchases to
              secure the best deals.
            </p>
          </div>
          <div>
            <h2 className="mb-3 text-base font-bold md:mb-4 md:text-md">
              What is Pay4more?
            </h2>
            <p>
              Pay4more allows you to buy items instantly at discounts of 50% or
              more. This platform is perfect for those who prefer a quick
              shopping experience without the need for bidding. Enjoy great
              deals with immediate purchase options.
            </p>
          </div>
          <div>
            <h2 className="mb-3 text-base font-bold md:mb-4 md:text-md">
              What is Fix4more?
            </h2>
            <p>
              Fix4more enables you to purchase items with minor defects at
              discounted prices. After fixing them, you can resell these items
              for a profit. It's a great way to save money and potentially earn
              from your skills.
            </p>
          </div>
          <div>
            <h2 className="mb-3 text-base font-bold md:mb-4 md:text-md">
              How can I contact?
            </h2>
            <p>
              You can reach out to us through our contact form or email. We are
              here to assist you with any inquiries. Your questions are
              important to us, and we aim to respond promptly.
            </p>
          </div>
        </div>
        <div className="mx-auto mt-12 max-w-md text-center md:mt-18 lg:mt-20">
          <h4 className="mb-3 text-2xl font-bold md:mb-4 md:text-3xl md:leading-[1.3] lg:text-4xl">
            Still have questions?
          </h4>
          <p className="md:text-md">
            We're here to help you with any inquiries.
          </p>
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
