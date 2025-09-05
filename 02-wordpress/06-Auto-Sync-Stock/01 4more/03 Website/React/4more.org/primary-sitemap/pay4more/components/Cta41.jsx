"use client";

import { Button } from "@relume_io/relume-ui";
import React from "react";

export function Cta41() {
  return (
    <section id="relume" className="px-[5%] py-16 md:py-24 lg:py-28">
      <div className="container">
        <div className="relative flex flex-col justify-center p-8 md:p-12 lg:p-16">
          <div>
            <div className="w-full max-w-lg">
              <h2 className="mb-5 text-5xl font-bold text-text-alternative md:mb-6 md:text-7xl lg:text-8xl">
                Shop Instant Deals Today!
              </h2>
              <p className="text-text-alternative md:text-md">
                Discover unbeatable prices and enjoy hassle-free shopping with
                Pay4more's instant purchase options.
              </p>
            </div>
            <div className="mt-6 flex flex-wrap items-center gap-4 md:mt-8">
              <Button title="Shop" variant="primary">
                Shop
              </Button>
              <Button title="Learn More" variant="secondary-alt">
                Learn More
              </Button>
            </div>
          </div>
          <div className="absolute inset-0 -z-10">
            <img
              src="https://d22po4pjz3o32e.cloudfront.net/placeholder-image.svg"
              className="size-full object-cover"
              alt="Relume placeholder image"
            />
            <div className="absolute inset-0 bg-black/50" />
          </div>
        </div>
      </div>
    </section>
  );
}
